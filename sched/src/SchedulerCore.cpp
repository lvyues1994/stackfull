#include <stackfull/sched/detail/SchedulerCore.h>

#include "Unwind.h"

#include <stackfull/coro/Fatal.h>
#include <stackfull/coro/detail/Sanitizer.h>
#include <stackfull/sched/detail/JoinState.h>
#include <stackfull/sched/detail/Runtime.h>
#include <stackfull/sched/detail/TaskFactory.h>
#include <stackfull/sched/detail/Worker.h>
#include <stackfull/stack/DefaultStackAllocator.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>

namespace stackfull {
namespace sched {
namespace detail {

namespace {

std::size_t resolveWorkerCount(std::size_t const requested) noexcept {
    std::size_t count = requested != 0 ? requested : std::thread::hardware_concurrency();
    if (count == 0) {
        count = 1;
    }
    return count > kMaxWorkers ? kMaxWorkers : count;
}

#if STACKFULL_HAS_EXCEPTIONS
struct AbortingExceptionSink final : ExceptionSink {
    void onUnhandledException(std::exception_ptr) noexcept override {
        coro::fatal("stackfull: unhandled exception escaped a detached task");
    }
};

ExceptionSink &abortingExceptionSink() noexcept {
    static AbortingExceptionSink sink;
    return sink;
}
#endif

bool popFreeSlot(SlotQueue &freeSlots, std::uint32_t &slot) noexcept {
    for (unsigned spins = 0;; ++spins) {
        queue::PopStatus const status = freeSlots.pop(slot);
        if (status == queue::PopStatus::Ok) {
            return true;
        }
        if (status == queue::PopStatus::Empty) {
            return false;
        }
        if (spins > 64) {
            std::this_thread::yield();
        }
    }
}

void pushFreeSlot(SlotQueue &freeSlots, std::uint32_t const slot) noexcept {
    for (unsigned spins = 0;; ++spins) {
        if (freeSlots.push(slot) == queue::PushStatus::Ok) {
            return;
        }
        if (spins > 64) {
            std::this_thread::yield();
        }
    }
}

struct SlotCacheGuard {
    explicit SlotCacheGuard(Worker::SlotCache &cache_) noexcept : cache(cache_) {
        while (cache.locked.exchange(true, std::memory_order_acquire)) {
            while (cache.locked.load(std::memory_order_relaxed)) {
                std::this_thread::yield();
            }
        }
    }
    ~SlotCacheGuard() { cache.locked.store(false, std::memory_order_release); }
    SlotCacheGuard(SlotCacheGuard const &) = delete;
    SlotCacheGuard &operator=(SlotCacheGuard const &) = delete;
    Worker::SlotCache &cache;
};

void finishJoin(JoinState &join) noexcept {
    // seq_cst on both sides makes this a Dekker pair with JoinHandle::join():
    // either we see the registered waiter or the joiner sees `done`.
    join.done.store(true, std::memory_order_seq_cst);
    std::uint8_t const kind = join.waiterKind.load(std::memory_order_seq_cst);
    if (kind == JoinState::kTaskWaiter) {
        join.taskWaiter.wake();
    } else if (kind == JoinState::kThreadWaiter) {
        join.threadWaiter->unpark();
    }
    join.release();
}

} // namespace

SchedulerCore::SchedulerCore(SchedulerOptions const &options_) : options(options_) {
    options.workers = resolveWorkerCount(options.workers);
    STACKFULL_CHECK(options.maxTasks >= 1 and options.maxTasks <= SlotQueue::kCapacity,
                    "stackfull: SchedulerOptions::maxTasks out of range for the injection queue");
    allocator = options.allocator != nullptr ? options.allocator : &stack::defaultStackAllocator();
    if (options.reserveStacks != 0) {
        allocator->reserve(options.taskStackSize, options.reserveStacks);
    }
    driver = options.driver;
#if STACKFULL_HAS_EXCEPTIONS
    exceptionSink = options.exceptionSink != nullptr ? options.exceptionSink : &abortingExceptionSink();
#endif

    slots = std::make_unique<TaskSlot[]>(options.maxTasks);
    for (std::uint32_t i = 0; i < options.maxTasks; ++i) {
        pushFreeSlot(freeSlots, i);
    }

    workers.reserve(options.workers);
    for (std::size_t i = 0; i < options.workers; ++i) {
        workers.push_back(std::make_unique<Worker>(*this, i));
    }
}

SchedulerCore::~SchedulerCore() {
    // Tasks that were spawned but never ran (no worker ever started) still
    // own their stacks; release them here. Everything else is gone once the
    // workers have exited.
    for (std::uint32_t i = 0; i < options.maxTasks; ++i) {
        Task *const task = slots[i].task.load(std::memory_order_acquire);
        if (task != nullptr) {
            destroyTaskEntry(*task);
            releaseTask(*task);
        }
    }
}

// Producer side of two Dekker pairs, done with RMWs on the very words the
// workers RMW (not fences): in the modification order of `searching` /
// `idleMask` our RMW is either after the worker's — then we observe it — or
// before it, and the worker's RMW then reads-from ours and inherits the
// happens-before with the task we just published. ThreadSanitizer models
// this; it does not model fences.
void SchedulerCore::notifyIdleWorker() noexcept {
    if (searching.fetch_add(0, std::memory_order_seq_cst) > 0) {
        return; // a searching worker will find the work without a futex
    }
    std::uint64_t mask = idleMask.fetch_or(0, std::memory_order_seq_cst);
    while (mask != 0) {
        std::uint64_t candidates = mask;
        if (Worker const *const keeper = timekeeper.load(std::memory_order_relaxed)) {
            std::uint64_t const keeperBit = std::uint64_t{1} << keeper->index;
            if ((candidates & ~keeperBit) != 0) {
                candidates &= ~keeperBit; // leave it asleep on the timers
            }
        }
        auto const index = static_cast<std::size_t>(__builtin_ctzll(candidates));
        std::uint64_t const bit = std::uint64_t{1} << index;
        if (idleMask.compare_exchange_weak(mask, mask & ~bit, std::memory_order_acq_rel)) {
            handOffTo(*workers[index]);
            return;
        }
    }
}

void SchedulerCore::notifyWorker(Worker &worker) noexcept {
    if (clearIdle(worker)) {
        handOffTo(worker);
    }
    // Otherwise it is running and will drain its inbox at the next switch.
}

void SchedulerCore::handOffTo(Worker &worker) noexcept {
    if (currentWorker() != nullptr) {
        worker.handoffWake.store(true, std::memory_order_relaxed);
    }
    unparkWorker(worker);
}

bool SchedulerCore::tryBecomeTimekeeper(Worker &worker) noexcept {
    Worker *expected = nullptr;
    return timekeeper.compare_exchange_strong(expected, &worker, std::memory_order_seq_cst);
}

void SchedulerCore::releaseTimekeeper(Worker &worker) noexcept {
    Worker *expected = &worker;
    timekeeper.compare_exchange_strong(expected, nullptr, std::memory_order_seq_cst);
}

void SchedulerCore::unparkWorker(Worker &worker) noexcept {
    // The timekeeper may be asleep inside the Driver rather than its Parker;
    // poke both — a surplus Parker token only makes a later park() return
    // early once.
    if (driver != nullptr and timekeeper.load(std::memory_order_seq_cst) == &worker) {
        driver->wake();
    }
    worker.parker->unpark();
}

// Against a worker that claims the timekeeper role, marks keeperDeadline as
// scanning, reads every queue's earliest deadline and publishes its own: our
// queue's mirror store precedes our loads, its mark precedes its reads, all
// seq_cst. So either its scan sees our entry, or we see the mark (or the
// deadline it computed without us) and wake it.
void SchedulerCore::addTimer(TimerEntry &entry) noexcept {
    Worker *const me = currentWorker();
    STACKFULL_CHECK(me != nullptr, "stackfull: timers are added from tasks only");
    me->timers.add(entry);
    if (Worker *const keeper = timekeeper.load(std::memory_order_seq_cst)) {
        if (ticksOf(entry.deadline) < keeperDeadline.load(std::memory_order_seq_cst)) {
            unparkWorker(*keeper); // it would sleep past our deadline
        }
        return;
    }
    // Nobody sleeps on the timers. The calling worker claims the role once
    // it runs out of work; if it already has more lined up, a peer must.
    if (me->hasLocalWork()) {
        ensureTimekeeper();
    }
}

// A worker woken here may find leftover work before it gets to claim the
// role, and would then call this again from its own dispatch: without a
// limit, one burst wakes worker after worker. So at most one such wakeup
// per rampUpDelay; timers stay covered, just up to that much later.
void SchedulerCore::ensureTimekeeper() noexcept {
    if (timekeeper.load(std::memory_order_seq_cst) != nullptr or not hasIdleWorkers() or not hasPendingTimers()) {
        return;
    }
    if (options.rampUpDelay.count() > 0) {
        std::int64_t const now = ticksOf(std::chrono::steady_clock::now());
        std::int64_t last = lastKeeperWake.load(std::memory_order_relaxed);
        std::int64_t const spacing =
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(options.rampUpDelay).count();
        if (now - last < spacing or not lastKeeperWake.compare_exchange_strong(last, now, std::memory_order_relaxed)) {
            return;
        }
    }
    notifyIdleWorker(); // a worker already searching claims the role when it sleeps
}

// Same protocol as addTimer(): the request is published (seq_cst) before
// the timekeeper and its deadline are read, and the timekeeper reads it
// after marking itself as scanning.
void SchedulerCore::requestRampUp() noexcept {
    if (options.rampUpDelay.count() <= 0) {
        notifyIdleWorker();
        return;
    }
    if (not hasIdleWorkers()) {
        return;
    }
    std::int64_t const due =
        ticksOf(std::chrono::steady_clock::now() +
                std::chrono::duration_cast<std::chrono::steady_clock::duration>(options.rampUpDelay));
    std::int64_t current = rampUpDeadline.load(std::memory_order_relaxed);
    do {
        if (current <= due) {
            return; // an earlier request is pending
        }
    } while (not rampUpDeadline.compare_exchange_weak(current, due, std::memory_order_seq_cst,
                                                      std::memory_order_relaxed));
    if (Worker *const keeper = timekeeper.load(std::memory_order_seq_cst)) {
        if (due < keeperDeadline.load(std::memory_order_seq_cst)) {
            unparkWorker(*keeper);
        }
        return;
    }
    notifyIdleWorker(); // nobody sleeps with a timeout to honour it
}

bool SchedulerCore::hasPendingTimers() const noexcept {
    return timerMask.load(std::memory_order_relaxed) != 0;
}

bool SchedulerCore::nextDeadline(TimePoint &out) const noexcept {
    std::int64_t earliest = kNoDeadline;
    for (std::uint64_t mask = timerMask.load(std::memory_order_seq_cst); mask != 0; mask &= mask - 1) {
        auto const index = static_cast<std::size_t>(__builtin_ctzll(mask));
        std::int64_t const mine = workers[index]->timers.earliestTicks();
        earliest = mine < earliest ? mine : earliest;
    }
    if (earliest == kNoDeadline) {
        return false;
    }
    out = TimePoint(TimePoint::duration(earliest));
    return true;
}

// Every busy worker calls this periodically: only non-empty queues are
// looked at, and those with nothing due are skipped without the lock.
void SchedulerCore::fireTimers() noexcept {
    std::uint64_t mask = timerMask.load(std::memory_order_acquire);
    if (mask == 0) {
        return;
    }
    TimePoint const now = std::chrono::steady_clock::now();
    for (; mask != 0; mask &= mask - 1) {
        TimerQueue &queue = workers[static_cast<std::size_t>(__builtin_ctzll(mask))]->timers;
        if (queue.earliestTicks() <= ticksOf(now)) {
            queue.fireExpired(now);
        }
    }
}

void SchedulerCore::markIdle(Worker &worker) noexcept {
    idleMask.fetch_or(std::uint64_t{1} << worker.index, std::memory_order_seq_cst);
}

bool SchedulerCore::clearIdle(Worker &worker) noexcept {
    std::uint64_t const bit = std::uint64_t{1} << worker.index;
    return (idleMask.fetch_and(~bit, std::memory_order_seq_cst) & bit) != 0;
}

Task *SchedulerCore::popInjection() noexcept {
    Task *task = nullptr;
    for (unsigned spins = 0; spins < 16; ++spins) {
        queue::PopStatus const status = injection.pop(task);
        if (status == queue::PopStatus::Ok) {
            return task;
        }
        if (status == queue::PopStatus::Empty) {
            return nullptr;
        }
        // Busy: a producer is between allocate and commit.
    }
    return nullptr;
}

void SchedulerCore::releaseTask(Task &task) noexcept {
    JoinState *const join = task.join;
#if STACKFULL_HAS_EXCEPTIONS
    std::exception_ptr escaped = std::move(task.exception);
    task.exception = nullptr;
    if (join != nullptr) {
        join->exception = std::move(escaped);
        finishJoin(*join);
    } else if (escaped) {
        exceptionSink->onUnhandledException(std::move(escaped));
    }
#else
    if (join != nullptr) {
        finishJoin(*join);
    }
#endif

    std::uint32_t const slot = task.slot;
    TaskSlot &entry = slots[slot];
    task.parkState.store(raw(TaskState::Done), std::memory_order_release);
    entry.task.store(nullptr, std::memory_order_release);
    entry.generation.fetch_add(1, std::memory_order_acq_rel);
    // Wakers that already hold the Task pointer finish within a few
    // instructions; wait them out before the memory goes away.
    while (entry.pins.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }

    stack::StackView const stack = task.stack;
    stack::StackAllocator *const stackAllocator = task.allocator;
    coro::detail::tsanDestroyFiber(task);
    task.~Task();
    stackAllocator->deallocate(stack);
    Worker *const me = currentWorker();
    // Counted as finished before its slot is free: when acquireSlot() finds
    // no slot yet sees fewer than maxTasks live tasks, one is on its way.
    countFinished(me);
    releaseSlot(me, slot);

    if (stopping.load(std::memory_order_acquire)) {
        // Finishers that race here RMW the same word: the later one reads
        // from the earlier and so sees its count, and the last to finish sees
        // zero. (Workers would still notice within a timed park.)
        stopping.exchange(true, std::memory_order_acq_rel);
        if (liveTasks() == 0) {
            for (auto const &worker : workers) {
                unparkWorker(*worker); // let everyone observe "no tasks left" and exit
            }
        }
    }
}

// The global free list is a shared MPMC queue; each worker keeps a small
// LIFO of slots in front of it and trades them in batches. Recently freed
// slots are reused first, keeping their TaskSlot entries in this core's cache.
bool SchedulerCore::acquireSlot(Worker *const me, std::uint32_t &slot) noexcept {
    for (;;) {
        if (tryAcquireSlot(me, slot)) {
            return true;
        }
        // Slots can be in transit between a cache and the global list; give
        // up only when maxTasks tasks really are alive.
        if (liveTasks() >= options.maxTasks) {
            return false;
        }
        std::this_thread::yield();
    }
}

bool SchedulerCore::tryAcquireSlot(Worker *const me, std::uint32_t &slot) noexcept {
    if (me != nullptr) {
        Worker::SlotCache &cache = me->slotCache;
        SlotCacheGuard const guard(cache);
        if (cache.count == 0) {
            while (cache.count < Worker::SlotCache::kBatch and popFreeSlot(freeSlots, cache.slots[cache.count])) {
                ++cache.count;
            }
        }
        if (cache.count != 0) {
            slot = cache.slots[--cache.count];
            return true;
        }
    } else if (popFreeSlot(freeSlots, slot)) {
        return true;
    }
    // The global list is dry: take a slot some worker is holding, so that
    // spawn() fails only once all maxTasks are really in use.
    for (auto const &worker : workers) {
        if (worker.get() == me) {
            continue;
        }
        Worker::SlotCache &cache = worker->slotCache;
        SlotCacheGuard const guard(cache);
        if (cache.count != 0) {
            slot = cache.slots[--cache.count];
            return true;
        }
    }
    return popFreeSlot(freeSlots, slot); // one may have been handed back meanwhile
}

void SchedulerCore::releaseSlot(Worker *const me, std::uint32_t const slot) noexcept {
    if (me == nullptr) {
        pushFreeSlot(freeSlots, slot);
        return;
    }
    Worker::SlotCache &cache = me->slotCache;
    SlotCacheGuard const guard(cache);
    if (cache.count == Worker::SlotCache::kCapacity) {
        // Full: hand the oldest half back, keep the recently used ones.
        constexpr std::uint32_t kBatch = Worker::SlotCache::kBatch;
        for (std::uint32_t i = 0; i < kBatch; ++i) {
            pushFreeSlot(freeSlots, cache.slots[i]);
        }
        for (std::uint32_t i = kBatch; i < cache.count; ++i) {
            cache.slots[i - kBatch] = cache.slots[i];
        }
        cache.count -= kBatch;
    }
    cache.slots[cache.count++] = slot;
}

std::size_t SchedulerCore::liveTasks() const noexcept {
    // Finished counts first, created counts second: a finish that is seen
    // happened after its creation, which is then seen too. The difference
    // can therefore only overcount (a task created during the scan), never
    // report zero while a task lives.
    std::uint64_t finished = foreignFinished.load(std::memory_order_acquire);
    for (auto const &worker : workers) {
        finished += worker->tasksFinished.load(std::memory_order_acquire);
    }
    std::uint64_t created = foreignCreated.load(std::memory_order_acquire);
    for (auto const &worker : workers) {
        created += worker->tasksCreated.load(std::memory_order_acquire);
    }
    return static_cast<std::size_t>(created - finished);
}

void SchedulerCore::wakeAllParked() noexcept {
    for (std::uint32_t i = 0; i < options.maxTasks; ++i) {
        TaskSlot &entry = slots[i];
        entry.pins.fetch_add(1, std::memory_order_acq_rel);
        Task *const task = entry.task.load(std::memory_order_acquire);
        if (task != nullptr) {
            wake(*task);
        }
        entry.pins.fetch_sub(1, std::memory_order_release);
    }
}

} // namespace detail
} // namespace sched
} // namespace stackfull
