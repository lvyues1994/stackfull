#include <stackfull/io/Async.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>
#include <stackfull/io/Tcp.h>
#include <stackfull/io/TcpStream.h>
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
                    // Counted before the reply goes out: the client may finish
                    // (and the test check the count) as soon as it arrives.
                    echoed.fetch_add(1);
                    IoResult const sent = io::writeAll(registration, buffer, got.bytes);
                    if (not sent) {
                        return;
                    }
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

// The registration outlives the task waiting on it: stop() unwinds the task
// (or, without exceptions, releases its stack outright) and readiness shows
// up only afterwards. Delivering it must not touch the task's stack.
TEST_P(IoTest, ReadinessAfterTheWaiterIsGoneIsHarmless) {
    int ends[2] = {-1, -1};
    ASSERT_EQ(::pipe(ends), 0);
    Fd readEnd{ends[0]};
    Fd writeEnd{ends[1]};
    ASSERT_FALSE(setNonBlocking(readEnd.get()));
    Registration registration(*poller, readEnd.get());
    // The waiter runs on a scheduler of its own; the fixture's scheduler
    // keeps polling and delivers the late readiness.
    SchedulerOptions options;
    options.workers = 1;
    auto waiterScheduler = makeScheduler(options);
    std::atomic<bool> waiting{false};
    ASSERT_TRUE(waiterScheduler->spawn([&] {
        waiting.store(true);
        char buffer[4];
        io::read(registration, buffer, sizeof buffer); // never completes
    }));
    while (not waiting.load()) {
        std::this_thread::sleep_for(milliseconds{1});
    }
    std::this_thread::sleep_for(milliseconds{10});
    waiterScheduler->stop();
    while (waiterScheduler->liveTasks() != 0) {
        std::this_thread::sleep_for(milliseconds{1});
    }
    ASSERT_EQ(::write(writeEnd.get(), "x", 1), 1);
    std::this_thread::sleep_for(milliseconds{30}); // delivered to a waker whose task is gone
    char buffer[4];
    EXPECT_EQ(::read(readEnd.get(), buffer, sizeof buffer), 1); // nobody consumed it
}

TEST_P(IoTest, TcpStreamEchoesBufferedWrites) {
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(bound) << bound.error.message();
    TcpListener &listener = *bound.listener;
    WaitGroup done;
    done.add(2);
    std::string expected;
    for (int i = 0; i < 1000; ++i) {
        expected += "msg-" + std::to_string(i) + "\n";
    }
    std::string echoed;
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpStreamResult peer = listener.accept();
        EXPECT_TRUE(peer);
        if (peer) {
            char buffer[512];
            for (;;) {
                IoResult const got = peer.stream->read(buffer, sizeof buffer);
                if (not got or got.bytes == 0) {
                    break;
                }
                peer.stream->writeAll(buffer, got.bytes);
            }
        }
        done.done(); // dropping the stream closes it: the client sees EOF
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpStreamResult client = TcpStream::connect(*poller, SocketAddress::loopback(listener.port()));
        EXPECT_TRUE(client) << client.error.message();
        if (client) {
            BufWriter out(*client.stream, 256);
            for (int i = 0; i < 1000; ++i) {
                std::string const line = "msg-" + std::to_string(i) + "\n";
                EXPECT_TRUE(out.write(line.data(), line.size()));
            }
            EXPECT_TRUE(out.flush());
            EXPECT_FALSE(client.stream->shutdownWrite());
            char buffer[512];
            for (;;) {
                IoResult const got = client.stream->read(buffer, sizeof buffer);
                if (not got or got.bytes == 0) {
                    break;
                }
                echoed.append(buffer, got.bytes);
            }
        }
        done.done();
    }));
    done.wait();
    EXPECT_EQ(echoed, expected);
}

TEST_P(IoTest, ReadForTimesOutOnAnIdleConnection) {
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    TcpListener &listener = *bound.listener;
    WaitGroup done;
    done.add(2);
    std::error_code firstRead;
    milliseconds waited{0};
    std::string later;
    std::atomic<bool> timedOut{false};
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpStreamResult peer = listener.accept();
        EXPECT_TRUE(peer);
        while (not timedOut.load()) {
            this_task::sleepFor(milliseconds{1});
        }
        if (peer) {
            peer.stream->writeAll("late", 4);
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpStreamResult client = TcpStream::connect(*poller, SocketAddress::loopback(listener.port()));
        EXPECT_TRUE(client);
        if (client) {
            char buffer[8];
            auto const start = std::chrono::steady_clock::now();
            firstRead = client.stream->readFor(milliseconds{20}, buffer, sizeof buffer).error;
            waited = std::chrono::duration_cast<milliseconds>(std::chrono::steady_clock::now() - start);
            timedOut.store(true);
            IoResult const got = client.stream->readExactly(buffer, 4); // a timeout leaves the stream usable
            later.assign(buffer, got.bytes);
        }
        done.done();
    }));
    done.wait();
    EXPECT_EQ(firstRead, std::errc::timed_out);
    EXPECT_GE(waited.count(), 20);
    EXPECT_EQ(later, "late");
}

TEST_P(IoTest, AcceptForTimesOut) {
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    WaitGroup done;
    done.add(1);
    std::error_code error;
    ASSERT_TRUE(scheduler->spawn([&] {
        error = bound.listener->acceptFor(milliseconds{15}).error;
        done.done();
    }));
    done.wait();
    EXPECT_EQ(error, std::errc::timed_out);
}

TEST_P(IoTest, WritevAllSendsEveryVector) {
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    TcpListener &listener = *bound.listener;
    constexpr std::size_t kChunk = 512 * 1024;
    std::vector<std::string> chunks = {std::string(kChunk, 'a'), std::string(kChunk, 'b'), std::string(kChunk, 'c')};
    WaitGroup done;
    done.add(2);
    std::size_t received = 0;
    bool inOrder = true;
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpStreamResult peer = listener.accept();
        EXPECT_TRUE(peer);
        if (peer) {
            char buffer[64 * 1024];
            for (;;) {
                IoResult const got = peer.stream->read(buffer, sizeof buffer);
                if (not got or got.bytes == 0) {
                    break;
                }
                for (std::size_t i = 0; i < got.bytes; ++i) {
                    inOrder = inOrder and buffer[i] == "abc"[(received + i) / kChunk];
                }
                received += got.bytes;
                this_task::yield(); // a slow reader: the writer sees short writes
            }
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpStreamResult client = TcpStream::connect(*poller, SocketAddress::loopback(listener.port()));
        EXPECT_TRUE(client);
        if (client) {
            iovec vectors[3];
            for (std::size_t i = 0; i < 3; ++i) {
                vectors[i] = iovec{&chunks[i][0], kChunk};
            }
            IoResult const sent = client.stream->writevAll(vectors, 3);
            EXPECT_TRUE(sent);
            EXPECT_EQ(sent.bytes, 3 * kChunk);
        }
        done.done();
    }));
    done.wait();
    EXPECT_EQ(received, 3 * kChunk);
    EXPECT_TRUE(inOrder);
}

namespace {

// A plain blocking client socket connected to 127.0.0.1:port.
Fd connectBlocking(std::uint16_t const port) {
    Fd socket{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
    SocketAddress const address = SocketAddress::loopback(port);
    EXPECT_EQ(::connect(socket.get(), address.get(), address.length), 0);
    return socket;
}

} // namespace

// A short read leaves the stream exhausted; data arriving afterwards must
// still be read (it brings a new report).
TEST_P(IoTest, ShortReadWaitsForTheNextArrival) {
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    TcpListener &listener = *bound.listener;
    WaitGroup done;
    done.add(1);
    std::string first;
    std::string second;
    milliseconds waited{0};
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpStreamResult peer = listener.accept();
        EXPECT_TRUE(peer);
        if (peer) {
            char buffer[64];
            IoResult got = peer.stream->read(buffer, sizeof buffer);
            first.assign(buffer, got.bytes);
            auto const start = std::chrono::steady_clock::now();
            got = peer.stream->readFor(std::chrono::seconds{5}, buffer, sizeof buffer);
            waited = std::chrono::duration_cast<milliseconds>(std::chrono::steady_clock::now() - start);
            second.assign(buffer, got.bytes);
        }
        done.done();
    }));
    Fd client = connectBlocking(listener.port());
    ASSERT_EQ(::write(client.get(), "abc", 3), 3);
    std::this_thread::sleep_for(milliseconds{30});
    ASSERT_EQ(::write(client.get(), "def", 3), 3);
    done.wait();
    EXPECT_EQ(first, "abc");
    EXPECT_EQ(second, "def");
    EXPECT_GE(waited.count(), 15);
}

// Data and the peer's FIN reported together: the short read of the data
// must not make the next read wait for a report that will never come.
TEST_P(IoTest, DataAndEndOfStreamArrivingTogetherAreBothRead) {
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    TcpListener &listener = *bound.listener;
    WaitGroup accepted;
    accepted.add(1);
    WaitGroup done;
    done.add(1);
    std::atomic<bool> sent{false};
    std::string data;
    IoResult end{99, std::error_code{}};
    ASSERT_TRUE(scheduler->spawn([&] {
        TcpStreamResult peer = listener.accept();
        accepted.done();
        EXPECT_TRUE(peer);
        while (not sent.load()) {
            this_task::sleepFor(milliseconds{1});
        }
        this_task::sleepFor(milliseconds{30}); // the poller sees data and FIN before we read
        if (peer) {
            char buffer[64];
            IoResult const got = peer.stream->read(buffer, sizeof buffer);
            data.assign(buffer, got.bytes);
            end = peer.stream->readFor(std::chrono::seconds{2}, buffer, sizeof buffer);
        }
        done.done();
    }));
    Fd client = connectBlocking(listener.port());
    accepted.wait();
    ASSERT_EQ(::write(client.get(), "abc", 3), 3);
    ASSERT_EQ(::shutdown(client.get(), SHUT_WR), 0);
    sent.store(true);
    done.wait();
    EXPECT_EQ(data, "abc");
    EXPECT_FALSE(end.error) << end.error.message();
    EXPECT_EQ(end.bytes, 0u);
}

TEST_P(IoTest, PipeDataAndHangUpArrivingTogetherAreBothRead) {
    int ends[2] = {-1, -1};
    ASSERT_EQ(::pipe(ends), 0);
    Fd readEnd{ends[0]};
    Fd writeEnd{ends[1]};
    ASSERT_FALSE(setNonBlocking(readEnd.get()));
    ASSERT_EQ(::write(writeEnd.get(), "abc", 3), 3);
    writeEnd.reset();
    WaitGroup done;
    done.add(1);
    std::string data;
    IoResult end{99, std::error_code{}};
    ASSERT_TRUE(scheduler->spawn([&] {
        {
            Registration registration(*poller, readEnd.get());
            registration.setByteStream(true);
            this_task::sleepFor(milliseconds{30});
            char buffer[64];
            IoResult const got = io::read(registration, buffer, sizeof buffer);
            data.assign(buffer, got.bytes);
            end = io::readUntil(registration, buffer, sizeof buffer,
                                std::chrono::steady_clock::now() + std::chrono::seconds{2});
        }
        done.done();
    }));
    done.wait();
    EXPECT_EQ(data, "abc");
    EXPECT_FALSE(end.error) << end.error.message();
    EXPECT_EQ(end.bytes, 0u);
}

#if defined(__linux__)
INSTANTIATE_TEST_SUITE_P(Backends, IoTest, ::testing::Values(Backend::Epoll, Backend::Poll), backendName);
#else
INSTANTIATE_TEST_SUITE_P(Backends, IoTest, ::testing::Values(Backend::Poll), backendName);
#endif
