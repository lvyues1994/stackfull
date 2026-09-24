#include <stackfull/sched/detail/Worker.h>

#include "Unwind.h"

#include <stackfull/coro/Config.h>
#include <stackfull/coro/detail/Switch.h>
#include <stackfull/coro/detail/ThreadState.h>
#include <stackfull/sched/detail/Runtime.h>

#include <stackfull/coro/Fatal.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include <pthread.h>

namespace stackfull {
namespace sched {
namespace detail {

namespace {

// xorshift32: victim selection only needs to be cheap and non-pathological.
std::uint32_t nextRandom(std::uint32_t &state) noexcept {
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    return state;
}

// Every 61st dispatch looks at the injection queue first (Go's schedtick
// trick) so foreign work is never starved by a busy local queue.
constexpr unsigned kInjectionCheckPeriod = 61;

// Idle spinning is bounded by time, not iterations: a core just out of a
// deep idle state runs slowly, and an iteration budget there costs tens of
// microseconds. One spinner already spares producers their futex wake.
constexpr std::uint32_t kMaxSpinners = 2;
// From the second empty spin in a row, the next 2, 4, ... up to this many
// idle episodes go straight to sleep before spinning is tried again. A
// single miss (the peer was descheduled) does not stop a ping-pong.
constexpr unsigned kMaxSpinBackoff = 32;

// "sf-worker-<n>" in debuggers and top -H (at most 15 characters).
void nameCurrentThread(std::size_t const index) noexcept {
#if defined(__linux__) || defined(__QNX__)
    char name[16];
    std::snprintf(name, sizeof name, "sf-worker-%zu", index);
    ::pthread_setname_np(::pthread_self(), name);
#else
    static_cast<void>(index);
#endif
}

inline void cpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
    asm volatile("yield" ::: "memory");
#endif
}

} // namespace

void *Worker::operator new(std::size_t const size) {
    void *memory = nullptr;
    if (::posix_memalign(&memory, alignof(Worker), size) != 0) {
        coro::fatal("stackfull: out of memory creating a worker");
    }
    return memory;
}

void Worker::operator delete(void *const memory) noexcept {
    std::free(memory);
}

Worker::Worker(SchedulerCore &core_, std::size_t const index_)
    : core(core_), index(index_), rng(static_cast<std::uint32_t>(0x9E3779B9u * (index_ + 1))),
      parker(makeParker()) {
    timers.attachActiveMask(core.timerMask, std::uint64_t{1} << index_);
}

void Worker::run() {
    nameCurrentThread(index);
    if (core.options.onWorkerStart) {
        core.options.onWorkerStart(index);
    }
    thread = &coro::detail::currentThreadState();
    thread->worker = this;
    dispatcher = &thread->mainBlock;
    running.store(true, std::memory_order_release);
    core.workersRunning.fetch_add(1, std::memory_order_acq_rel);

    for (;;) {
        Task *task = nullptr;
        if ((tick % kInjectionCheckPeriod) == 0) {
            maintain();
            task = core.popInjection();
        }
        if (task == nullptr) {
            task = takeLocal();
        }
        if (task == nullptr) {
            task = searchForWork();
        }
        if (task == nullptr) {
            if (core.stopping.load(std::memory_order_acquire)) {
                // Not before stop() has woken every parked task once: a task
                // it has not reached yet must get to see stopRequested().
                if (core.parkedWoken.load(std::memory_order_acquire)) {
                    reapParkedTasks();
                }
                if (core.liveTasks() == 0) {
                    break;
                }
            }
            task = parkIdle();
            if (task == nullptr) {
                continue;
            }
        }
        ++tick;
        // The task (and whatever it hands off to) may keep this worker away
        // from its idle loop for a while; pending timers must not wait for it.
        if (core.timekeeper.load(std::memory_order_relaxed) == nullptr) {
            core.ensureTimekeeper();
        }
        coro::detail::switchTo(*dispatcher, *task); // returns when a task hands control back
    }

    core.workersRunning.fetch_sub(1, std::memory_order_acq_rel);
    running.store(false, std::memory_order_release);
    thread->worker = nullptr;
}

Task *Worker::steal() noexcept {
    std::size_t const count = core.workers.size();
    if (count <= 1) {
        return nullptr;
    }
    std::size_t const start = nextRandom(rng) % count;
    for (std::size_t i = 0; i < count; ++i) {
        Worker &victim = *core.workers[(start + i) % count];
        if (&victim == this) {
            continue;
        }
        Task *first = nullptr;
        if (local.minRemainingSlots() < LocalQueue::kEntriesPerBlock) {
            // Little room here: take a single task.
            if (victim.local.steal(first)) {
                bump(steals);
                return first;
            }
            continue;
        }
        Task *batch[LocalQueue::kEntriesPerBlock];
        std::size_t const stolen = victim.local.stealBlock(batch);
        if (stolen == 0) {
            continue;
        }
        bump(steals);
        std::size_t const pushed = local.pushBatch(batch + 1, batch + stolen);
        for (std::size_t j = 1 + pushed; j < stolen; ++j) {
            core.inject(*batch[j]); // cannot happen after the room check; kept total
        }
        return batch[0];
    }
    return nullptr;
}

// At most half the workers search (steal) at a time; the rest park until a
// searcher that finds work notifies one of them. The last searcher to find
// work wakes another worker so parallelism ramps up one step per find.
Task *Worker::searchForWork() noexcept {
    std::size_t const limit = core.workers.size() / 2 + 1;
    std::uint32_t current = core.searching.load(std::memory_order_acquire);
    bool searcher = false;
    while (current < limit) {
        if (core.searching.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel)) {
            searcher = true;
            break;
        }
    }

    Task *task = core.popInjection();
    if (task == nullptr and searcher) {
        task = steal();
        if (task == nullptr) {
            task = core.popInjection();
        }
    }

    if (searcher) {
        std::uint32_t const before = core.searching.fetch_sub(1, std::memory_order_acq_rel);
        if (task != nullptr and before == 1) {
            wakePeerForLeftovers();
        }
    }
    return task;
}

// Producers that saw a searcher skipped their wakeup, so the last searcher
// to find work wakes a peer — but only for work that is left over (more
// injected tasks, or the rest of a stolen batch). A lone task wakes nobody
// else. The fetch_sub on `searching` just before reads-from any such
// producer's RMW, so its push is visible to these probes.
void Worker::wakePeerForLeftovers() noexcept {
    if (core.hasInjectedWork() or hasLocalWork()) {
        core.requestRampUp();
    }
}

// Before sleeping, spin as a searcher for up to options.idleSpin. Producers skip
// the futex wake while any worker is searching, so back-to-back handoffs
// never pay a wake/sleep syscall pair. At most kMaxSpinners spin at once,
// and spins that keep coming up empty back off exponentially: work arriving
// less often than the budget costs a wakeup and almost no spinning, while a
// switch back to rapid handoffs is picked up within kMaxSpinBackoff idle
// episodes.
Task *Worker::spinForWork() noexcept {
    if (core.options.idleSpin.count() <= 0) {
        return nullptr;
    }
    // With nobody polling the driver we are next to: spinning would only
    // delay noticing IO readiness, which the spin does not look at.
    if (core.driver != nullptr and core.timekeeper.load(std::memory_order_relaxed) == nullptr) {
        return nullptr;
    }
    if (spinSkips != 0) {
        --spinSkips;
        return nullptr;
    }
    std::uint32_t current = core.searching.load(std::memory_order_relaxed);
    do {
        if (current >= kMaxSpinners) {
            return nullptr;
        }
    } while (not core.searching.compare_exchange_weak(current, current + 1, std::memory_order_seq_cst,
                                                      std::memory_order_relaxed));

    Task *task = nullptr;
    auto const deadline = std::chrono::steady_clock::now() + core.options.idleSpin;
    for (unsigned i = 1; task == nullptr; ++i) {
        task = takeLocal();
        if (task == nullptr and core.hasInjectedWork()) {
            task = core.popInjection();
        }
        if (task == nullptr and (i & 127u) == 0) {
            task = steal();
        }
        if (task == nullptr and (i & 31u) == 0 and std::chrono::steady_clock::now() >= deadline) {
            break;
        }
        cpuRelax();
    }

    std::uint32_t const before = core.searching.fetch_sub(1, std::memory_order_seq_cst);
    // We may have been the searcher a producer relied on: re-check after
    // stepping down. A producer that saw us searching did its RMW on
    // `searching` before ours, so our RMW reads-from it and its push is
    // visible here (see SchedulerCore::notifyIdleWorker).
    if (task == nullptr) {
        task = takeLocal();
    }
    if (task == nullptr) {
        task = core.popInjection();
    }
    if (task != nullptr) {
        spinMisses = 0;
        if (before == 1) {
            wakePeerForLeftovers();
        }
    } else {
        spinMisses = spinMisses < 16 ? spinMisses + 1 : spinMisses;
        if (spinMisses >= 2) {
            unsigned const backoff = 1u << (spinMisses - 1);
            spinSkips = backoff < kMaxSpinBackoff ? backoff : kMaxSpinBackoff;
        }
    }
    return task;
}

// Publish idleness (an RMW on idleMask), then look once more: a producer
// whose RMW on idleMask preceded ours did not see the bit, but ours then
// reads-from its RMW and its task is visible on this re-check. Returns work
// found before or instead of sleeping.
Task *Worker::parkIdle() noexcept {
    if (not core.stopping.load(std::memory_order_acquire)) {
        if (Task *const found = spinForWork()) {
            return found;
        }
    }

    core.markIdle(*this);

    Task *task = takeLocal();
    if (task == nullptr) {
        task = core.popInjection();
    }
    if (task != nullptr) {
        if (not core.clearIdle(*this)) {
            // Someone already claimed us and will unpark: consume that token
            // now so the next real park is not cut short.
            parker->parkFor(std::chrono::nanoseconds{0});
        }
        return task;
    }

    sleepIdle();
    core.clearIdle(*this);
    if (handoffWake.exchange(false, std::memory_order_relaxed)) {
        spinMisses = 0;
        spinSkips = 0;
    }
    // Off the idle mask first, so the batch is not handed back to us.
    return placeGathered(true);
}

// Periodic upkeep on a busy worker: fire due timers and, if nobody is
// sleeping in the Driver, give it a non-blocking poll so IO readiness is
// noticed even when every worker is busy.
void Worker::maintain() noexcept {
    gathering = true;
    core.fireTimers();
    if (core.driver != nullptr and core.tryBecomeTimekeeper(*this)) {
        core.driver->wait(std::chrono::nanoseconds{0});
        core.releaseTimekeeper(*this);
    }
    gathering = false;
    placeGathered(false);
}

Task *Worker::placeGathered(bool const keepFirst) noexcept {
    Task *task = gatheredHead;
    gatheredHead = nullptr;
    gatheredTail = nullptr;
    Task *kept = nullptr;
    if (task != nullptr and keepFirst) {
        kept = task;
        task = task->mpscNext.load(std::memory_order_relaxed);
    } else if (task != nullptr and task->mpscNext.load(std::memory_order_relaxed) == nullptr and not hasLocalWork()) {
        // A lone wakeup on a worker with nothing else queued runs here next;
        // waking a peer for it would only move it to a colder thread.
        pushLocalLifo(*task);
        return nullptr;
    }
    bool const feedPeers = core.hasIdleWorkers();
    bool injected = false;
    while (task != nullptr) {
        Task *const following = task->mpscNext.load(std::memory_order_relaxed);
        if (feedPeers) {
            core.pushInjection(*task);
            injected = true;
        } else {
            pushLocalFifo(*task);
        }
        task = following;
    }
    if (injected) {
        core.notifyIdleWorker(); // one peer; searchers wake more while work is left
    }
    return kept;
}

// The actual sleep of an idle worker. One worker becomes the timekeeper and
// sleeps with the earliest timer as timeout — inside the Driver when there
// is one — then fires what came due, gathering the tasks it wakes so that
// it runs one itself instead of waking a peer for it; the others sleep on
// their Parker until notified.
void Worker::sleepIdle() noexcept {
    bump(sleeps);
    bool const stopping = core.stopping.load(std::memory_order_acquire);
    if (not core.tryBecomeTimekeeper(*this)) {
        if (stopping) {
            parker->parkFor(std::chrono::milliseconds{1}); // re-scan for parked tasks
        } else {
            parker->park();
        }
        return;
    }

    std::chrono::nanoseconds timeout{-1};
    // While we look, any task adding a timer or requesting ramp-up wakes us
    // (see addTimer, requestRampUp).
    core.keeperDeadline.store(SchedulerCore::kScanningTimers, std::memory_order_seq_cst);
    TimePoint deadline;
    std::int64_t wakeAt = core.nextDeadline(deadline) ? ticksOf(deadline) : kNoDeadline;
    std::int64_t const rampUp = core.rampUpDeadline.load(std::memory_order_seq_cst);
    wakeAt = rampUp < wakeAt ? rampUp : wakeAt;
    if (wakeAt != kNoDeadline) {
        std::int64_t const now = ticksOf(std::chrono::steady_clock::now());
        timeout = std::chrono::nanoseconds{wakeAt > now ? wakeAt - now : 0};
    }
    core.keeperDeadline.store(wakeAt, std::memory_order_seq_cst);
    if (stopping and (timeout < std::chrono::nanoseconds{0} or timeout > std::chrono::milliseconds{1})) {
        timeout = std::chrono::milliseconds{1};
    }

    gathering = true;
    if (core.driver != nullptr) {
        core.driver->wait(timeout);
    } else if (timeout < std::chrono::nanoseconds{0}) {
        parker->park();
    } else {
        parker->parkFor(timeout);
    }
    core.releaseTimekeeper(*this);
    core.fireTimers();
    gathering = false;
    // A due ramp-up request is ours to answer: back in the run loop we take
    // leftover work if it is still there (and request the next step).
    std::int64_t due = core.rampUpDeadline.load(std::memory_order_acquire);
    if (due != kNoDeadline and due <= ticksOf(std::chrono::steady_clock::now())) {
        core.rampUpDeadline.compare_exchange_strong(due, kNoDeadline, std::memory_order_acq_rel);
    }
}

// Shutdown: claim every task still parked and end it. With exceptions the
// task unwinds through its park() call; otherwise its stack is released as
// is. A task that is Running elsewhere is left to its worker.
void Worker::reapParkedTasks() noexcept {
    for (std::uint32_t i = 0; i < core.options.maxTasks; ++i) {
        TaskSlot &entry = core.slots[i];
        entry.pins.fetch_add(1, std::memory_order_acq_rel);
        Task *const task = entry.task.load(std::memory_order_acquire);
        bool claimed = false;
        if (task != nullptr) {
            std::uint8_t expected = raw(TaskState::Parked);
            claimed = task->parkState.compare_exchange_strong(expected, raw(TaskState::Running),
                                                              std::memory_order_acq_rel);
        }
        entry.pins.fetch_sub(1, std::memory_order_release);
        if (not claimed) {
            continue;
        }
#if STACKFULL_HAS_EXCEPTIONS
        coro::detail::switchToOnTop(*dispatcher, *task, &throwForcedUnwindIntoTask);
#else
        destroyTaskEntry(*task);
        core.releaseTask(*task);
#endif
    }
}

} // namespace detail
} // namespace sched
} // namespace stackfull
