#pragma once

#include <stackfull/sched/ThisTask.h>
#include <stackfull/sched/detail/Runtime.h>
#include <stackfull/sched/detail/TimerQueue.h>

#include <atomic>
#include <chrono>

namespace stackfull {
namespace sched {
namespace this_task {

// Park the current task until `deadline`. The worker stays free for other
// tasks; the timekeeper worker fires the deadline. Only from inside a task.
inline void sleepUntil(std::chrono::steady_clock::time_point const deadline) {
    detail::CurrentTask const current = detail::currentTask();
    detail::TimerEntry entry;
    entry.deadline = deadline;
    entry.token = WakeToken{current.task.scheduler, current.task.slot, current.task.generation};
    current.worker.core.addTimer(entry);
    // Forced unwind while sleeping: the entry lives on this stack, so it
    // must leave the queue first. cancel() returning false means it already
    // fired, and fireExpired() never touches an entry after marking it.
    struct CancelOnUnwind {
        detail::TimerEntry &entry;
        bool armed = true;
        ~CancelOnUnwind() {
            if (armed) {
                detail::cancelTimer(entry);
            }
        }
    } cancelOnUnwind{entry};
    while (not entry.fired.load(std::memory_order_acquire)) {
        park(); // spurious wakeups just loop
    }
    cancelOnUnwind.armed = false;
}

template <class Rep, class Period>
inline void sleepFor(std::chrono::duration<Rep, Period> const duration) {
    sleepUntil(std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
}

} // namespace this_task
} // namespace sched
} // namespace stackfull
