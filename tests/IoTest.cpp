#include <stackfull/io/Async.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>
#include <stackfull/io/Tcp.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/Sleep.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/WaitGroup.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

using namespace stackfull;
using namespace stackfull::io;
using namespace stackfull::sched;
using stackfull::sync::WaitGroup;
using std::chrono::milliseconds;

namespace {

enum class Backend { Epoll, Poll };

std::unique_ptr<Poller> makePoller(Backend const backend) {
#if defined(__linux__)
    if (backend == Backend::Epoll) {
        return makeEpollPoller();
    }
#else
    static_cast<void>(backend);
#endif
    return makePollPoller();
}

struct IoTest : ::testing::TestWithParam<Backend> {
    void SetUp() override {
        poller = makePoller(GetParam());
        SchedulerOptions options;
        options.workers = 3;
        options.driver = poller.get();
        scheduler = makeScheduler(options);
        scheduler->start();
    }
    void TearDown() override {
        scheduler.reset(); // workers gone before the poller
        poller.reset();
    }

    std::unique_ptr<Poller> poller;
    std::unique_ptr<Scheduler> scheduler;
};

std::string backendName(::testing::TestParamInfo<Backend> const &info) {
    return info.param == Backend::Epoll ? "epoll" : "poll";
}

} // namespace

TEST_P(IoTest, ReadParksUntilDataArrives) {
    int ends[2] = {-1, -1};
    ASSERT_EQ(::pipe(ends), 0);
    Fd readEnd{ends[0]};
    Fd writeEnd{ends[1]};
    ASSERT_FALSE(setNonBlocking(readEnd.get()));

    WaitGroup done;
    done.add(1);
    std::string received;
    milliseconds waited{0};
    ASSERT_TRUE(scheduler->spawn([&] {
        {
            Registration registration(*poller, readEnd.get());
            ASSERT_FALSE(registration.error());
            char buffer[16];
            auto const start = std::chrono::steady_clock::now();
            IoResult const result = io::read(registration, buffer, sizeof buffer);
            waited = std::chrono::duration_cast<milliseconds>(std::chrono::steady_clock::now() - start);
            ASSERT_TRUE(result) << result.error.message();
            received.assign(buffer, result.bytes);
        } // unregister before the test body closes the descriptor
        done.done();
    }));

    std::this_thread::sleep_for(milliseconds{30});
    ASSERT_EQ(::write(writeEnd.get(), "hello", 5), 5);
    done.wait();
    EXPECT_EQ(received, "hello");
    EXPECT_GE(waited.count(), 25);
}

TEST_P(IoTest, ReadReportsEndOfStream) {
    int ends[2] = {-1, -1};
    ASSERT_EQ(::pipe(ends), 0);
    Fd readEnd{ends[0]};
    Fd writeEnd{ends[1]};
    ASSERT_FALSE(setNonBlocking(readEnd.get()));
    WaitGroup done;
    done.add(1);
    std::size_t bytes = 99;
    ASSERT_TRUE(scheduler->spawn([&] {
        {
            Registration registration(*poller, readEnd.get());
            char buffer[8];
            IoResult const result = io::read(registration, buffer, sizeof buffer);
            ASSERT_TRUE(result);
            bytes = result.bytes;
        }
        done.done();
    }));
    std::this_thread::sleep_for(milliseconds{10});
    writeEnd.reset(); // EOF
    done.wait();
    EXPECT_EQ(bytes, 0u);
}

TEST_P(IoTest, WriteParksUntilThePipeDrains) {
    int ends[2] = {-1, -1};
    ASSERT_EQ(::pipe(ends), 0);
    Fd readEnd{ends[0]};
    Fd writeEnd{ends[1]};
    ASSERT_FALSE(setNonBlocking(writeEnd.get()));
    ASSERT_FALSE(setNonBlocking(readEnd.get()));

    constexpr std::size_t kTotal = 4 * 1024 * 1024; // far beyond the pipe buffer
    std::vector<char> payload(kTotal, 'x');
    WaitGroup done;
    done.add(1);
    IoResult written;
    ASSERT_TRUE(scheduler->spawn([&] {
        {
            Registration registration(*poller, writeEnd.get());
            written = io::writeAll(registration, payload.data(), payload.size());
        }
        done.done();
    }));

    std::size_t drained = 0;
    std::vector<char> sink(65536);
    while (drained < kTotal) {
        ssize_t const n = ::read(readEnd.get(), sink.data(), sink.size());
        if (n > 0) {
            drained += static_cast<std::size_t>(n);
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds{200});
        }
    }
    done.wait();
    ASSERT_TRUE(written) << written.error.message();
    EXPECT_EQ(written.bytes, kTotal);
}

TEST_P(IoTest, TcpEchoManyConnections) {
    TcpListenResult listener = listenTcpLoopback();
    ASSERT_TRUE(listener) << listener.error.message();
    std::uint16_t const port = listener.port;

    constexpr int kClients = 32;
    constexpr int kRoundsPerClient = 50;
    WaitGroup clientsDone;
    clientsDone.add(kClients);
    std::atomic<int> echoed{0};
    std::atomic<bool> stopServer{false};

    // Server: accept loop spawning an echo task per connection.
    ASSERT_TRUE(scheduler->spawn([&] {
        Registration acceptor(*poller, listener.fd.get());
        for (int accepted = 0; accepted < kClients; ++accepted) {
            AcceptResult client = io::accept(acceptor);
            ASSERT_TRUE(client) << client.error.message();
            int const fd = client.fd.release();
            ASSERT_TRUE(scheduler->spawn([fd, &echoed, this] {
                Fd connection{fd};
                Registration registration(*poller, connection.get());
                char buffer[256];
                for (;;) {
                    IoResult const got = io::read(registration, buffer, sizeof buffer);
                    if (not got or got.bytes == 0) {
                        return;
                    }
                    IoResult const sent = io::writeAll(registration, buffer, got.bytes);
                    if (not sent) {
                        return;
                    }
                    echoed.fetch_add(1);
                }
            }));
        }
    }));

    for (int c = 0; c < kClients; ++c) {
        ASSERT_TRUE(scheduler->spawn([&, c] {
            TcpSocketResult socket = makeTcpSocket();
            ASSERT_TRUE(socket) << socket.error.message();
            Registration registration(*poller, socket.fd.get());
            sockaddr_in const address = loopbackAddress(port);
            std::error_code const connected =
                io::connect(registration, reinterpret_cast<sockaddr const *>(&address), sizeof address);
            ASSERT_FALSE(connected) << connected.message();
            for (int round = 0; round < kRoundsPerClient; ++round) {
                std::string const message = "client " + std::to_string(c) + " round " + std::to_string(round);
                ASSERT_TRUE(io::writeAll(registration, message.data(), message.size()));
                std::string reply(message.size(), '\0');
                IoResult const got = io::readExactly(registration, &reply[0], reply.size());
                ASSERT_TRUE(got);
                ASSERT_EQ(got.bytes, message.size());
                ASSERT_EQ(reply, message);
            }
            clientsDone.done();
        }));
    }
    clientsDone.wait();
    EXPECT_EQ(echoed.load(), kClients * kRoundsPerClient);
    stopServer.store(true);
}

TEST_P(IoTest, ConnectToClosedPortFails) {
    TcpListenResult listener = listenTcpLoopback();
    ASSERT_TRUE(listener);
    std::uint16_t const port = listener.port;
    listener.fd.reset(); // nothing listens there now

    WaitGroup done;
    done.add(1);
    std::error_code connectError;
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpSocketResult socket = makeTcpSocket();
        ASSERT_TRUE(socket);
        Registration registration(*poller, socket.fd.get());
        sockaddr_in const address = loopbackAddress(port);
        connectError = io::connect(registration, reinterpret_cast<sockaddr const *>(&address), sizeof address);
        done.done();
    }));
    done.wait();
    EXPECT_EQ(connectError, std::errc::connection_refused);
}

// A CPU-bound yielding task on the only worker must not starve IO: the
// dispatcher's periodic maintenance polls the driver without blocking.
TEST_P(IoTest, IoProgressesWhileTheOnlyWorkerIsBusyYielding) {
    scheduler.reset();
    SchedulerOptions options;
    options.workers = 1;
    options.driver = poller.get();
    scheduler = makeScheduler(options);
    scheduler->start();

    int ends[2] = {-1, -1};
    ASSERT_EQ(::pipe(ends), 0);
    Fd readEnd{ends[0]};
    Fd writeEnd{ends[1]};
    ASSERT_FALSE(setNonBlocking(readEnd.get()));

    WaitGroup done;
    done.add(2);
    std::atomic<bool> stop{false};
    std::string received;
    ASSERT_TRUE(scheduler->spawn([&] {
        while (not stop.load()) {
            this_task::yield();
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        {
            Registration registration(*poller, readEnd.get());
            char buffer[8];
            IoResult const result = io::read(registration, buffer, sizeof buffer);
            ASSERT_TRUE(result);
            received.assign(buffer, result.bytes);
        }
        stop.store(true);
        done.done();
    }));
    std::this_thread::sleep_for(milliseconds{20});
    ASSERT_EQ(::write(writeEnd.get(), "ping", 4), 4);
    done.wait();
    EXPECT_EQ(received, "ping");
}

TEST_P(IoTest, TimersAndIoCoexist) {
    int ends[2] = {-1, -1};
    ASSERT_EQ(::pipe(ends), 0);
    Fd readEnd{ends[0]};
    Fd writeEnd{ends[1]};
    ASSERT_FALSE(setNonBlocking(readEnd.get()));

    WaitGroup done;
    done.add(2);
    milliseconds slept{0};
    std::string received;
    ASSERT_TRUE(scheduler->spawn([&] {
        auto const start = std::chrono::steady_clock::now();
        this_task::sleepFor(milliseconds{15});
        slept = std::chrono::duration_cast<milliseconds>(std::chrono::steady_clock::now() - start);
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        {
            Registration registration(*poller, readEnd.get());
            char buffer[8];
            IoResult const result = io::read(registration, buffer, sizeof buffer);
            ASSERT_TRUE(result);
            received.assign(buffer, result.bytes);
        }
        done.done();
    }));
    std::this_thread::sleep_for(milliseconds{40});
    ASSERT_EQ(::write(writeEnd.get(), "io", 2), 2);
    done.wait();
    EXPECT_GE(slept.count(), 15);
    EXPECT_LT(slept.count(), 15 + 30); // the poller's wait must respect the timer deadline
    EXPECT_EQ(received, "io");
}

TEST_P(IoTest, WaitFromPlainThreadWorksToo) {
    int ends[2] = {-1, -1};
    ASSERT_EQ(::pipe(ends), 0);
    Fd readEnd{ends[0]};
    Fd writeEnd{ends[1]};
    ASSERT_FALSE(setNonBlocking(readEnd.get()));
    Registration registration(*poller, readEnd.get());

    std::thread writer([&] {
        std::this_thread::sleep_for(milliseconds{20});
        ssize_t const n = ::write(writeEnd.get(), "t", 1);
        static_cast<void>(n);
    });
    char buffer[4];
    IoResult const result = io::read(registration, buffer, sizeof buffer); // main thread blocks on its Parker
    writer.join();
    ASSERT_TRUE(result);
    EXPECT_EQ(result.bytes, 1u);
}

#if defined(__linux__)
INSTANTIATE_TEST_SUITE_P(Backends, IoTest, ::testing::Values(Backend::Epoll, Backend::Poll), backendName);
#else
INSTANTIATE_TEST_SUITE_P(Backends, IoTest, ::testing::Values(Backend::Poll), backendName);
#endif
