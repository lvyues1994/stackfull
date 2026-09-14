#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/Fatal.h>
#include <stackfull/coro/detail/Switch.h>
#include <stackfull/coro/detail/ThreadState.h>
#include <stackfull/sched/detail/SchedulerCore.h>
#include <stackfull/sched/detail/Task.h>
#include <stackfull/sched/detail/Worker.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>

// Inline hot paths of the scheduler: local queue operations, placement,
// park/wake, and the post-switch hooks. Everything here runs on a worker
// thread and, like the coroutine switch itself, touches TLS only through the
// single currentThreadState() lookup at the public API boundary.

namespace stackfull {
namespace sched {
namespace detail {

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

STACKFULL_ALWAYS_INLINE Task *Worker::takeLocal() noexcept {
    // The LIFO slot wins unless it has been used kLifoCap times in a row
    // while the queue had work — then the queue gets a turn.
    constexpr unsigned kLifoCap = 3;
    if (lifoSlot != nullptr and (lifoStreak < kLifoCap or not local.hasEntries())) {
        Task *const task = lifoSlot;
        lifoSlot = nullptr;
        ++lifoStreak;
        return task;
    }
    lifoStreak = 0;
    Task *task = nullptr;
    if (local.pop(task)) {
        return task;
    }
    if (Task *const pinned = pinnedInbox.pop()) {
        return pinned;
    }
    if (lifoSlot != nullptr) { // the queue turned out empty after all
        Task *const task2 = lifoSlot;
        lifoSlot = nullptr;
        return task2;
    }
    return nullptr;
}

STACKFULL_ALWAYS_INLINE coro::detail::ContextBlock &Worker::nextOrDispatcher() noexcept {
    Task *const task = takeLocal();
    return task != nullptr ? static_cast<coro::detail::ContextBlock &>(*task) : *dispatcher;
}

inline void Worker::pushLocalFifo(Task &task) noexcept {
    if (local.push(&task)) {
        return;
    }
    // Full. Hand a whole block to the injection queue unless thieves are
    // already relieving us, in which case only the newcomer goes global.
    if (not local.producerBlockHasActiveStealers()) {
        Task *batch[LocalQueue::kEntriesPerBlock];
        std::size_t const count = local.popBlock(batch);
        for (std::size_t i = 0; i < count; ++i) {
            core.inject(*batch[i]);
        }
    }
    core.inject(task);
}

inline void Worker::pushLocalLifo(Task &task) noexcept {
    if (lifoSlot != nullptr) {
        pushLocalFifo(*lifoSlot);
    }
    lifoSlot = &task;
}

inline bool Worker::hasLocalWork() const noexcept {
    return lifoSlot != nullptr or local.hasEntries() or not pinnedInbox.isEmpty();
}

// ---------------------------------------------------------------------------
// SchedulerCore
// ---------------------------------------------------------------------------

inline Worker *SchedulerCore::currentWorker() noexcept {
    coro::ThreadState &thread = coro::detail::currentThreadState();
    auto *const worker = static_cast<Worker *>(thread.worker);
    return (worker != nullptr and &worker->core == this) ? worker : nullptr;
}

inline bool SchedulerCore::hasIdleWorkers() const noexcept {
    return idleMask.load(std::memory_order_acquire) != 0;
}

inline void SchedulerCore::inject(Task &task) noexcept {
    for (unsigned spins = 0;; ++spins) {
        queue::PushStatus const status = injection.push(&task);
        if (status == queue::PushStatus::Ok) {
            break;
        }
        STACKFULL_CHECK(status != queue::PushStatus::Full,
                        "stackfull: injection queue full — more live tasks than maxTasks");
        if (spins > 64) {
            std::this_thread::yield(); // another thread is between allocate and commit
        }
    }
    notifyIdleWorker();
}

// Placement policy. Pinned tasks go to their worker. Otherwise: a worker
// with idle peers hands the task to one of them through the injection queue
// (BWoS cannot steal from a short queue, so idle workers must be fed, not
// left to steal); a fully busy worker keeps it local in the LIFO slot; a
// foreign thread always injects.
inline void SchedulerCore::schedule(Task &task) noexcept {
    if (task.pinnedTo != nullptr) {
        Worker &target = *task.pinnedTo;
        target.pinnedInbox.push(task);
        notifyWorker(target); // RMW on idleMask: orders the push against the worker's idle re-check
        return;
    }
    Worker *const me = currentWorker();
    if (me != nullptr and not hasIdleWorkers()) {
        me->pushLocalLifo(task);
        return;
    }
    inject(task);
}

inline void SchedulerCore::wake(Task &task) noexcept {
    std::uint8_t state = task.parkState.load(std::memory_order_acquire);
    for (;;) {
        if (state == raw(TaskState::Notified) or state == raw(TaskState::Done)) {
            return; // already woken, or nothing left to wake
        }
        if (task.parkState.compare_exchange_weak(state, raw(TaskState::Notified), std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
            if (state == raw(TaskState::Parked)) {
                // Fully switched out: it is ours to run again.
                task.parkState.store(raw(TaskState::Running), std::memory_order_relaxed);
                schedule(task);
            }
            // Was Running: it has not parked yet; onTaskParked will see the
            // token and reschedule without ever sleeping.
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Post-switch hooks
// ---------------------------------------------------------------------------

inline void onTaskYielded(coro::detail::ContextBlock &suspended, void *const arg) noexcept {
    Worker &worker = *static_cast<Worker *>(arg);
    Task &task = static_cast<Task &>(suspended);
    // A pinned task may only wait in its worker's unstealable inbox.
    if (task.pinnedTo != nullptr) {
        task.pinnedTo->pinnedInbox.push(task);
        return;
    }
    worker.pushLocalFifo(task);
}

inline void onTaskParked(coro::detail::ContextBlock &suspended, void *const arg) noexcept {
    Worker &worker = *static_cast<Worker *>(arg);
    Task &task = static_cast<Task &>(suspended);
    std::uint8_t const previous = task.parkState.exchange(raw(TaskState::Parked), std::memory_order_acq_rel);
    if (previous == raw(TaskState::Notified)) {
        // A wake raced ahead of the switch; the task never really sleeps.
        task.parkState.store(raw(TaskState::Running), std::memory_order_relaxed);
        worker.core.schedule(task);
    }
}

inline void onTaskFinished(coro::detail::ContextBlock &finished, void *const arg) noexcept {
    Worker &worker = *static_cast<Worker *>(arg);
    worker.core.releaseTask(static_cast<Task &>(finished));
}

// ---------------------------------------------------------------------------
// Current task lookup for the this_task API
// ---------------------------------------------------------------------------

struct CurrentTask {
    Task &task;
    Worker &worker;
};

STACKFULL_ALWAYS_INLINE CurrentTask currentTask() noexcept {
    coro::ThreadState &thread = coro::detail::currentThreadState();
    auto *const worker = static_cast<Worker *>(thread.worker);
    STACKFULL_CHECK(worker != nullptr and not coro::detail::isNativeContext(thread, *thread.current),
                    "stackfull: this_task API used outside a scheduled task");
    return CurrentTask{static_cast<Task &>(*thread.current), *worker};
}

inline bool isInsideTask() noexcept {
    coro::ThreadState const &thread = coro::detail::currentThreadState();
    return thread.worker != nullptr and not coro::detail::isNativeContext(thread, *thread.current);
}

} // namespace detail
} // namespace sched
} // namespace stackfull
