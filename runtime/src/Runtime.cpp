#include <stackfull/runtime/Runtime.h>

#include <stackfull/io/Poller.h>
#include <stackfull/sched/Scheduler.h>

namespace stackfull {

namespace {

struct DefaultRuntime {
    io::Poller *poller;
    sched::Scheduler *scheduler;
};

// Immortal: threads that outlive static destruction may still call into it.
DefaultRuntime const &defaultRuntime() {
    static DefaultRuntime const runtime = [] {
        io::Poller *const poller = io::makeDefaultPoller().release();
        sched::SchedulerOptions options;
        options.driver = poller;
        return DefaultRuntime{poller, sched::makeScheduler(options).release()};
    }();
    return runtime;
}

} // namespace

sched::Scheduler &defaultScheduler() {
    return *defaultRuntime().scheduler;
}

io::Poller &defaultPoller() {
    return *defaultRuntime().poller;
}

} // namespace stackfull
