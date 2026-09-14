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
#include <cstdlib>

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
      parker(makeParker()) {}

void Worker::run() {
    thread = &coro::detail::currentThreadState();
    thread->worker = this;
    dispatcher = &thread->mainBlock;
    running.store(true, std::memory_order_release);
    core.workersRunning.fetch_add(1, std::memory_order_acq_rel);

    for (;;) {
        Task *task = nullptr;
        if ((tick % kInjectionCheckPeriod) == 0) {
            task = core.popInjection();
        }
        if (task == nullptr) {
            task = takeLocal();
        }
        if (task == nullptr) {
            task = searchForWork();
        }
        if (task != nullptr) {
            ++tick;
            coro::detail::switchTo(*dispatcher, *task);
            continue; // some task handed control back to us
        }

        if (core.stopping.load(std::memory_order_acquire)) {
            reapParkedTasks();
            if (core.liveTasks.load(std::memory_order_acquire) == 0) {
                break;
            }
        }
        if (Task *const found = parkIdle()) {
            ++tick;
            coro::detail::switchTo(*dispatcher, *found);
        }
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
                return first;
            }
            continue;
        }
        Task *batch[LocalQueue::kEntriesPerBlock];
        std::size_t const stolen = victim.local.stealBlock(batch);
        if (stolen == 0) {
            continue;
        }
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
            core.notifyIdleWorker();
        }
    }
    return task;
}

// Before sleeping, spin as a searcher for a while. Producers skip the futex
// wake while any worker is searching, so a steady trickle of work never pays
// a wake/sleep syscall pair per task. The spin is bounded (tens of µs) and
// skipped during shutdown.
Task *Worker::spinForWork() noexcept {
    constexpr unsigned kSpinIterations = 2000;
    core.searching.fetch_add(1, std::memory_order_seq_cst);
    Task *task = nullptr;
    for (unsigned i = 0; i < kSpinIterations and task == nullptr; ++i) {
        task = takeLocal();
        if (task == nullptr and (i & 7u) == 0) {
            task = core.popInjection();
        }
        if (task == nullptr and (i & 63u) == 63) {
            task = steal();
        }
    }
    core.searching.fetch_sub(1, std::memory_order_seq_cst);
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

    if (core.stopping.load(std::memory_order_acquire)) {
        parker->parkFor(std::chrono::milliseconds{1}); // re-scan for parked tasks
    } else {
        parker->park();
    }
    core.clearIdle(*this);
    return nullptr;
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
