#include <stackfull/runtime/Runtime.h>

#include <stackfull/sched/Scheduler.h>

namespace stackfull {

sched::Scheduler &defaultScheduler() {
    // Immortal: threads that outlive static destruction may still call into it.
    static sched::Scheduler *const instance = makeScheduler(sched::SchedulerOptions{}).release();
    return *instance;
}

} // namespace stackfull
