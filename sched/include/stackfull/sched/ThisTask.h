#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/detail/Switch.h>
#include <stackfull/sched/WakeToken.h>
#include <stackfull/sched/detail/Runtime.h>

#include <cstddef>
#include <cstdint>

// Operations available to the body of a scheduled task. All of them are
// inline so that the switch continues straight into the caller's code (an
// out-of-line wrapper would add one return-stack-buffer mispredict per switch).

namespace stackfull {
namespace sched {
namespace this_task {

// Let other runnable tasks go first; the caller re-enters at the tail of the
// local queue. Injected work is pulled into the local queue when it is empty
// (and, for fairness, once every 64 yields regardless), so a pair of
// yielding tasks hands off directly without touching the global queue. With
// nothing runnable at all the call is almost free; every 64th such call
// still visits the dispatcher so it can steal from busy workers.
STACKFULL_ALWAYS_INLINE void yield() {
    detail::CurrentTask const current = detail::currentTask();
    detail::Worker &worker = current.worker;
    // Counts as progress for the stall monitor, even when nothing else is
    // runnable; owner-only, so a relaxed load and store (a plain increment).
    unsigned const yields = worker.yieldTick.load(std::memory_order_relaxed) + 1;
    worker.yieldTick.store(yields, std::memory_order_relaxed);
    bool const fairnessTick = (yields & 63u) == 0;
    if (fairnessTick) {
        if (detail::Task *const injected = worker.core.popInjection()) {
            worker.pushLocalFifo(*injected);
        }
    }
    detail::Task *next = worker.takeLocal();
    if (next == nullptr) {
        next = worker.core.popInjection();
    }
    if (next == nullptr and not fairnessTick) {
        return;
    }
    coro::detail::ContextBlock &target =
        next != nullptr ? static_cast<coro::detail::ContextBlock &>(*next) : *worker.dispatcher;
    current.task.postSwitch = coro::detail::PostSwitchHook{&detail::onTaskYielded, &worker};
    coro::detail::switchTo(current.task, target);
}

// Sleep until WakeToken::wake() (or a wake that already arrived). May return
// spuriously; loop on your condition.
STACKFULL_ALWAYS_INLINE void park() {
    detail::CurrentTask const current = detail::currentTask();
    detail::Task &task = current.task;
    detail::countSwitch(current.worker);
    if (task.parkState.load(std::memory_order_acquire) == detail::raw(detail::TaskState::Notified)) {
        task.parkState.store(detail::raw(detail::TaskState::Running), std::memory_order_relaxed);
        return; // consume the pending token without switching
    }
    task.postSwitch = coro::detail::PostSwitchHook{&detail::onTaskParked, &current.worker};
    coro::detail::switchTo(task, current.worker.nextOrDispatcher());
}

inline WakeToken token() noexcept {
    detail::CurrentTask const current = detail::currentTask();
    return WakeToken{current.task.scheduler, current.task.slot, current.task.generation};
}

// True once Scheduler::stop() was requested or requestStop() targeted this
// task. CPU-bound loops must poll this; only parked tasks can be stopped
// from outside.
inline bool stopRequested() noexcept {
    detail::CurrentTask const current = detail::currentTask();
    return current.task.stopRequested or current.worker.core.stopping.load(std::memory_order_relaxed);
}

// Bind the task to the worker it is currently on: it will not migrate until
// unpin(). Wakes from other threads go through that worker's inbox.
inline void pinToCurrentWorker() noexcept {
    detail::CurrentTask const current = detail::currentTask();
    current.task.pinnedTo = &current.worker;
}

inline void unpin() noexcept {
    detail::CurrentTask const current = detail::currentTask();
    current.task.pinnedTo = nullptr;
}

inline bool isTask() noexcept {
    return detail::isInsideTask();
}

inline std::size_t workerIndex() noexcept {
    return detail::currentTask().worker.index;
}

} // namespace this_task
} // namespace sched
} // namespace stackfull
