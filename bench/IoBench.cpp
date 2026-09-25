// IO and timer micro benchmarks.
//
//   stackfull_io_bench [iterations]

#include <stackfull/io/Async.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>
#include <stackfull/io/Tcp.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/Sleep.h>
#include <stackfull/sync/WaitGroup.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>

using namespace stackfull;
using namespace stackfull::io;
using namespace stackfull::sched;
using stackfull::sync::WaitGroup;

namespace {

using Clock = std::chrono::steady_clock;

double nanosPerOp(Clock::time_point const start, Clock::time_point const end, long const ops) {
    auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return static_cast<double>(ns) / static_cast<double>(ops);
}

void report(char const *const name, double const nsPerOp, char const *const note) {
    std::printf("%-52s %9.1f ns  %s\n", name, nsPerOp, note);
}

// One byte back and forth over a loopback TCP connection between two tasks.
void benchEchoRoundTrip(char const *const name, std::unique_ptr<Poller> poller, std::size_t const workers,
                        long const rounds) {
    SchedulerOptions options;
    options.workers = workers;
    options.driver = poller.get();
    auto scheduler = makeScheduler(options);

    TcpListenResult listener = listenTcpLoopback();
    std::uint16_t const port = listener.port;
    WaitGroup done;
    done.add(2);
    Clock::time_point start;
    Clock::time_point end;

    scheduler->spawn([&] {
        Registration acceptor(*poller, listener.fd.get());
        AcceptResult client = io::accept(acceptor);
        Registration connection(*poller, client.fd.get());
        connection.setKind(Registration::Kind::StreamSocket);
        char byte = 0;
        for (long i = 0; i < rounds; ++i) {
            if (io::readExactly(connection, &byte, 1).bytes != 1) {
                break;
            }
            io::writeAll(connection, &byte, 1);
        }
        done.done();
    });
    scheduler->spawn([&] {
        TcpSocketResult socket = makeTcpSocket();
        Registration connection(*poller, socket.fd.get());
        connection.setKind(Registration::Kind::StreamSocket);
        sockaddr_in const address = loopbackAddress(port);
        io::connect(connection, reinterpret_cast<sockaddr const *>(&address), sizeof address);
        char byte = 'x';
        start = Clock::now();
        for (long i = 0; i < rounds; ++i) {
            io::writeAll(connection, &byte, 1);
            io::readExactly(connection, &byte, 1);
        }
        end = Clock::now();
        done.done();
    });
    scheduler->start();
    done.wait();
    report(name, nanosPerOp(start, end, rounds), "per round trip");
}

// Many tasks sleeping 1 ms in a loop: timer add/fire throughput.
void benchSleepChurn(std::size_t const workers, int const tasks, long const roundsPerTask) {
    SchedulerOptions options;
    options.workers = workers;
    auto scheduler = makeScheduler(options);
    WaitGroup done;
    done.add(static_cast<std::size_t>(tasks));
    auto const start = Clock::now();
    for (int t = 0; t < tasks; ++t) {
        scheduler->spawn([&] {
            for (long i = 0; i < roundsPerTask; ++i) {
                this_task::sleepFor(std::chrono::microseconds{200});
            }
            done.done();
        });
    }
    scheduler->start();
    done.wait();
    auto const end = Clock::now();
    char name[80];
    std::snprintf(name, sizeof name, "sleepFor(200us) x %d tasks, %zu workers", tasks, workers);
    report(name, nanosPerOp(start, end, roundsPerTask * tasks), "per timer (wall / total timers)");
}

} // namespace

int main(int const argc, char **const argv) {
    long const iterations = argc > 1 ? std::atol(argv[1]) : 100'000L;
    std::printf("iterations: %ld\n", iterations);
#if defined(__linux__)
    benchEchoRoundTrip("TCP echo round trip, epoll, 2 workers", makeEpollPoller(), 2, iterations);
    benchEchoRoundTrip("TCP echo round trip, epoll, 1 worker", makeEpollPoller(), 1, iterations);
#endif
    benchEchoRoundTrip("TCP echo round trip, poll,  2 workers", makePollPoller(), 2, iterations);
    benchSleepChurn(2, 100, iterations / 100);
    return 0;
}
