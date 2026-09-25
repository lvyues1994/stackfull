#include <stackfull/runtime/Runtime.h>

#include <stackfull/io/Poller.h>
#include <stackfull/sched/Scheduler.h>

#include <string>

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

io::ResolveResult resolve(char const *const host, std::uint16_t const port, int const family) {
    io::SocketAddress numeric;
    if (io::SocketAddress::parse(host, port, numeric)) {
        return io::resolve(host, port, family);
    }
    // By value: a task unwound at stop() leaves the job running without it.
    std::string const name(host);
    return blocking([name, port, family] { return io::resolve(name.c_str(), port, family); });
}

io::TcpStreamResult connectTcp(char const *const host, std::uint16_t const port, io::Deadline const deadline) {
    io::ResolveResult const resolved = resolve(host, port);
    if (not resolved) {
        return io::TcpStreamResult{nullptr, resolved.error};
    }
    return io::TcpStream::connect(defaultPoller(), resolved.addresses, deadline);
}

} // namespace stackfull
