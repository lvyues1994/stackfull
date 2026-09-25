// Network throughput and latency: echo servers and a load generator.
//
//   stackfull_net_bench server <port> <workers> [lifetime-seconds]
//       echo server, one task per connection (TcpListener / TcpStream)
//   stackfull_net_bench rawserver <port> <threads> [lifetime-seconds]
//       baseline: a thread per core, each with its own SO_REUSEPORT listener
//       and edge-triggered epoll, echo in callbacks
//   stackfull_net_bench client <port> <connections> <bytes> <seconds> <workers>
//       ping-pong: each connection sends `bytes` and waits for them back
//   stackfull_net_bench churn <port> <tasks> <seconds> <workers>
//       short connections: connect, one 64-byte round trip, close
//   stackfull_net_bench stream <port> <chunk> <seconds>
//       one connection, a writer and a reader task: bytes/s through the echo
//   stackfull_net_bench hol <workers>
//       head-of-line: one connection's handler burns 20 ms per request while
//       a second connection pings every millisecond (clients are threads)

#include <stackfull/io/Poller.h>
#include <stackfull/io/TcpStream.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sync/WaitGroup.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/epoll.h>
#endif

using namespace stackfull;
using namespace stackfull::io;
using namespace stackfull::sched;

namespace {

using Clock = std::chrono::steady_clock;

std::unique_ptr<Scheduler> makeNetScheduler(Poller &poller, std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    options.driver = &poller;
    return makeScheduler(options);
}

// Servers exit on their own after `lifetime` seconds (0: never), printing
// the process's resource usage to stderr.
[[noreturn]] void serveFor(double const lifetime) {
    if (lifetime > 0) {
        std::this_thread::sleep_for(std::chrono::duration<double>(lifetime));
        rusage usage{};
        ::getrusage(RUSAGE_SELF, &usage);
        std::fprintf(stderr, "RUSAGE user %.3f sys %.3f voluntary %ld involuntary %ld\n",
                     static_cast<double>(usage.ru_utime.tv_sec) + 1e-6 * static_cast<double>(usage.ru_utime.tv_usec),
                     static_cast<double>(usage.ru_stime.tv_sec) + 1e-6 * static_cast<double>(usage.ru_stime.tv_usec),
                     usage.ru_nvcsw, usage.ru_nivcsw);
        std::fflush(stdout);
        std::_Exit(0);
    }
    for (;;) {
        std::this_thread::sleep_for(std::chrono::hours{1});
    }
}

void printLatency(char const *const name, std::vector<float> &samples) {
    if (samples.empty()) {
        std::printf("%s: no samples\n", name);
        return;
    }
    std::sort(samples.begin(), samples.end());
    auto const at = [&](double const q) {
        return samples[std::min(samples.size() - 1, static_cast<std::size_t>(q * static_cast<double>(samples.size())))];
    };
    auto const slow = samples.end() - std::upper_bound(samples.begin(), samples.end(), 1000.0f);
    std::printf("%s  p50 %8.1f  p99 %8.1f  p99.9 %8.1f  max %9.1f us  >1ms %.1f%%\n", name, at(0.5), at(0.99),
                at(0.999), samples.back(), 100.0 * static_cast<double>(slow) / static_cast<double>(samples.size()));
}

// --- stackfull echo server -------------------------------------------------------

int runServer(std::uint16_t const port, std::size_t const workers, double const lifetime) {
    auto poller = makeDefaultPoller();
    auto scheduler = makeNetScheduler(*poller, workers);
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::any(port), 4096);
    if (not bound) {
        std::fprintf(stderr, "bind: %s\n", bound.error.message().c_str());
        return 1;
    }
    TcpListener &listener = *bound.listener;
    scheduler->spawn([&] {
        for (;;) {
            TcpStreamResult accepted = listener.accept();
            if (not accepted) {
                continue;
            }
            scheduler->spawn([stream = std::move(accepted.stream)]() mutable {
                char buffer[16384];
                for (;;) {
                    IoResult const got = stream->read(buffer, sizeof buffer);
                    if (not got or got.bytes == 0) {
                        return;
                    }
                    if (not stream->writeAll(buffer, got.bytes)) {
                        return;
                    }
                }
            });
        }
    });
    serveFor(lifetime);
}

// --- epoll baseline ----------------------------------------------------------------

#if defined(__linux__)

int listenReusePort(std::uint16_t const port) {
    int const fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int const one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof address) != 0 or ::listen(fd, 4096) != 0) {
        std::perror("bind/listen");
        std::exit(1);
    }
    return fd;
}

// Per connection: bytes read but not yet written back.
struct RawConnection {
    int fd;
    std::string pending;
};

void flushPending(RawConnection &connection) {
    while (not connection.pending.empty()) {
        ssize_t const n = ::write(connection.fd, connection.pending.data(), connection.pending.size());
        if (n <= 0) {
            return; // EAGAIN: EPOLLOUT resumes
        }
        connection.pending.erase(0, static_cast<std::size_t>(n));
    }
}

bool serveReadable(RawConnection &connection, char *const buffer, std::size_t const size) {
    for (;;) {
        ssize_t const n = ::read(connection.fd, buffer, size);
        if (n == 0) {
            return false;
        }
        if (n < 0) {
            return errno == EAGAIN;
        }
        if (connection.pending.empty()) {
            ssize_t const sent = ::write(connection.fd, buffer, static_cast<std::size_t>(n));
            std::size_t const done = sent > 0 ? static_cast<std::size_t>(sent) : 0;
            connection.pending.append(buffer + done, static_cast<std::size_t>(n) - done);
        } else {
            connection.pending.append(buffer, static_cast<std::size_t>(n));
            flushPending(connection);
        }
        // A short read drained the socket (edge-triggered: new data brings a new edge).
        if (static_cast<std::size_t>(n) < size) {
            return true;
        }
    }
}

void rawLoop(std::uint16_t const port) {
    int const listener = listenReusePort(port);
    int const epollFd = ::epoll_create1(EPOLL_CLOEXEC);
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.ptr = nullptr;
    ::epoll_ctl(epollFd, EPOLL_CTL_ADD, listener, &event);
    char buffer[16384];
    epoll_event events[1024];
    for (;;) {
        int const count = ::epoll_wait(epollFd, events, 1024, -1);
        for (int i = 0; i < count; ++i) {
            auto *const connection = static_cast<RawConnection *>(events[i].data.ptr);
            if (connection == nullptr) {
                for (;;) {
                    int const fd = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (fd < 0) {
                        break;
                    }
                    int const one = 1;
                    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
                    epoll_event added{};
                    added.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
                    added.data.ptr = new RawConnection{fd, std::string()};
                    ::epoll_ctl(epollFd, EPOLL_CTL_ADD, fd, &added);
                }
                continue;
            }
            bool alive = true;
            if ((events[i].events & EPOLLOUT) != 0) {
                flushPending(*connection);
            }
            if ((events[i].events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0) {
                alive = serveReadable(*connection, buffer, sizeof buffer);
            }
            if (not alive) {
                ::close(connection->fd); // also leaves the epoll set
                delete connection;
            }
        }
    }
}

int runRawServer(std::uint16_t const port, std::size_t const threads, double const lifetime) {
    for (std::size_t i = 0; i < threads; ++i) {
        std::thread([port] { rawLoop(port); }).detach();
    }
    serveFor(lifetime);
}

#endif // __linux__

// --- load generator ------------------------------------------------------------------

int runClient(std::uint16_t const port, int const connections, std::size_t const bytes, double const seconds,
              std::size_t const workers) {
    auto poller = makeDefaultPoller();
    auto scheduler = makeNetScheduler(*poller, workers);
    std::atomic<bool> counting{false};
    std::atomic<bool> stop{false};
    std::atomic<int> connected{0};
    std::vector<std::vector<float>> latencies(static_cast<std::size_t>(connections));
    std::vector<long> trips(static_cast<std::size_t>(connections), 0);
    std::atomic<long> allTrips{0}; // warm-up included: to divide the server's rusage by
    sync::WaitGroup done;
    done.add(static_cast<std::size_t>(connections));
    for (int c = 0; c < connections; ++c) {
        scheduler->spawn([&, c] {
            std::vector<float> &mine = latencies[static_cast<std::size_t>(c)];
            mine.reserve(1 << 16);
            TcpStreamResult opened = TcpStream::connect(*poller, SocketAddress::loopback(port));
            if (not opened) {
                std::fprintf(stderr, "connect: %s\n", opened.error.message().c_str());
                std::_Exit(1);
            }
            TcpStream &stream = *opened.stream;
            std::vector<char> message(bytes, 'x');
            std::vector<char> reply(bytes);
            connected.fetch_add(1);
            long count = 0;
            long everything = 0;
            while (not stop.load(std::memory_order_relaxed)) {
                auto const start = Clock::now();
                if (not stream.writeAll(message.data(), bytes) or stream.readExactly(reply.data(), bytes).bytes != bytes) {
                    break;
                }
                ++everything;
                if (counting.load(std::memory_order_relaxed)) {
                    auto const took = std::chrono::duration<float, std::micro>(Clock::now() - start).count();
                    mine.push_back(took);
                    ++count;
                }
            }
            trips[static_cast<std::size_t>(c)] = count;
            allTrips.fetch_add(everything);
            done.done();
        });
    }
    while (connected.load() < connections) {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{500});
    counting.store(true);
    auto const start = Clock::now();
    std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
    counting.store(false);
    double const elapsed = std::chrono::duration<double>(Clock::now() - start).count();
    stop.store(true);
    done.wait();

    long total = 0;
    std::vector<float> all;
    for (int c = 0; c < connections; ++c) {
        total += trips[static_cast<std::size_t>(c)];
        all.insert(all.end(), latencies[static_cast<std::size_t>(c)].begin(), latencies[static_cast<std::size_t>(c)].end());
    }
    char name[96];
    std::snprintf(name, sizeof name, "%5d conns x %5zu B: %8.0f req/s ", connections, bytes,
                  static_cast<double>(total) / elapsed);
    printLatency(name, all);
    std::printf("round trips in all: %ld\n", allTrips.load());
    scheduler->stop();
    return 0;
}

// Short connections: each task connects, does one 64-byte round trip, closes.
int runChurn(std::uint16_t const port, int const tasks, double const seconds, std::size_t const workers) {
    auto poller = makeDefaultPoller();
    auto scheduler = makeNetScheduler(*poller, workers);
    std::atomic<bool> counting{false};
    std::atomic<bool> stop{false};
    std::atomic<long> completed{0};
    std::atomic<long> failed{0};
    sync::WaitGroup done;
    done.add(static_cast<std::size_t>(tasks));
    for (int t = 0; t < tasks; ++t) {
        scheduler->spawn([&] {
            char message[64] = {};
            while (not stop.load(std::memory_order_relaxed)) {
                TcpStreamResult opened = TcpStream::connect(*poller, SocketAddress::loopback(port));
                if (not opened or not opened.stream->writeAll(message, sizeof message) or
                    opened.stream->readExactly(message, sizeof message).bytes != sizeof message) {
                    failed.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (counting.load(std::memory_order_relaxed)) {
                    completed.fetch_add(1, std::memory_order_relaxed);
                }
            }
            done.done();
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{500});
    counting.store(true);
    auto const start = Clock::now();
    std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
    counting.store(false);
    double const elapsed = std::chrono::duration<double>(Clock::now() - start).count();
    stop.store(true);
    done.wait();
    std::printf("churn, %d tasks: %8.0f connections/s (%ld failed)\n", tasks,
                static_cast<double>(completed.load()) / elapsed, failed.load());
    scheduler->stop();
    return 0;
}

int runStream(std::uint16_t const port, std::size_t const chunk, double const seconds) {
    auto poller = makeDefaultPoller();
    auto scheduler = makeNetScheduler(*poller, 2);
    TcpStreamResult opened;
    sync::WaitGroup connected;
    connected.add(1);
    scheduler->spawn([&] {
        opened = TcpStream::connect(*poller, SocketAddress::loopback(port));
        connected.done();
    });
    connected.wait();
    if (not opened) {
        std::fprintf(stderr, "connect: %s\n", opened.error.message().c_str());
        return 1;
    }
    TcpStream &stream = *opened.stream;
    std::atomic<bool> stop{false};
    std::atomic<long long> received{0};
    sync::WaitGroup done;
    done.add(2);
    scheduler->spawn([&] {
        std::vector<char> data(chunk, 'x');
        while (not stop.load(std::memory_order_relaxed)) {
            if (not stream.writeAll(data.data(), chunk)) {
                break;
            }
        }
        stream.shutdownWrite();
        done.done();
    });
    scheduler->spawn([&] {
        std::vector<char> data(chunk);
        for (;;) {
            IoResult const got = stream.read(data.data(), chunk);
            if (not got or got.bytes == 0) {
                break;
            }
            received.fetch_add(static_cast<long long>(got.bytes), std::memory_order_relaxed);
        }
        done.done();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    long long const before = received.load();
    auto const start = Clock::now();
    std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
    long long const after = received.load();
    double const elapsed = std::chrono::duration<double>(Clock::now() - start).count();
    stop.store(true);
    done.wait();
    std::printf("stream, %zu B chunks: %.2f GB/s through the echo\n", chunk,
                static_cast<double>(after - before) / elapsed / 1e9);
    opened.stream.reset();
    scheduler->stop();
    return 0;
}

// --- head-of-line blocking -------------------------------------------------------------

int blockingConnect(std::uint16_t const port) {
    int const fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof address) != 0) {
        std::perror("connect");
        std::exit(1);
    }
    int const one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}

bool roundTrip(int const fd, char const request) {
    char reply = 0;
    return ::write(fd, &request, 1) == 1 and ::read(fd, &reply, 1) == 1;
}

int runHeadOfLine(std::size_t const workers) {
    auto poller = makeDefaultPoller();
    auto scheduler = makeNetScheduler(*poller, workers);
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    TcpListener &listener = *bound.listener;
    scheduler->spawn([&] {
        for (;;) {
            TcpStreamResult accepted = listener.accept();
            if (not accepted) {
                return;
            }
            scheduler->spawn([stream = std::move(accepted.stream)]() mutable {
                char request = 0;
                while (stream->read(&request, 1).bytes == 1) {
                    if (request == 'H') {
                        auto const until = Clock::now() + std::chrono::milliseconds{20};
                        while (Clock::now() < until) {
                        }
                    }
                    if (not stream->writeAll(&request, 1)) {
                        return;
                    }
                }
            });
        }
    });

    std::atomic<bool> stop{false};
    std::thread heavy([&] {
        int const fd = blockingConnect(listener.port());
        while (not stop.load() and roundTrip(fd, 'H')) {
        }
        ::close(fd);
    });
    int const fd = blockingConnect(listener.port());
    std::vector<float> samples;
    auto next = Clock::now();
    auto const end = next + std::chrono::seconds{3};
    while (Clock::now() < end) {
        next += std::chrono::milliseconds{1};
        std::this_thread::sleep_until(next);
        auto const start = Clock::now();
        if (not roundTrip(fd, 'L')) {
            break;
        }
        samples.push_back(std::chrono::duration<float, std::micro>(Clock::now() - start).count());
    }
    stop.store(true);
    heavy.join();
    ::close(fd);
    char name[96];
    std::snprintf(name, sizeof name, "light pings next to a 20 ms handler, %zu workers:", workers);
    printLatency(name, samples);
    std::fflush(stdout);
    std::_Exit(0); // the accept loop is parked for good
}

} // namespace

int main(int const argc, char **const argv) {
    std::string const mode = argc > 1 ? argv[1] : "";
    auto const arg = [&](int const i, char const *const fallback) { return std::string(argc > i ? argv[i] : fallback); };
    if (mode == "server") {
        return runServer(static_cast<std::uint16_t>(std::stoi(arg(2, "9000"))), std::stoul(arg(3, "4")),
                         std::stod(arg(4, "0")));
    }
#if defined(__linux__)
    if (mode == "rawserver") {
        return runRawServer(static_cast<std::uint16_t>(std::stoi(arg(2, "9000"))), std::stoul(arg(3, "4")),
                            std::stod(arg(4, "0")));
    }
#endif
    if (mode == "client") {
        return runClient(static_cast<std::uint16_t>(std::stoi(arg(2, "9000"))), std::stoi(arg(3, "64")),
                         std::stoul(arg(4, "64")), std::stod(arg(5, "3")), std::stoul(arg(6, "8")));
    }
    if (mode == "churn") {
        return runChurn(static_cast<std::uint16_t>(std::stoi(arg(2, "9000"))), std::stoi(arg(3, "64")),
                        std::stod(arg(4, "3")), std::stoul(arg(5, "8")));
    }
    if (mode == "stream") {
        return runStream(static_cast<std::uint16_t>(std::stoi(arg(2, "9000"))), std::stoul(arg(3, "65536")),
                         std::stod(arg(4, "3")));
    }
    if (mode == "hol") {
        return runHeadOfLine(std::stoul(arg(2, "4")));
    }
    std::fprintf(stderr, "usage: see the comment at the top of bench/NetBench.cpp\n");
    return 2;
}
