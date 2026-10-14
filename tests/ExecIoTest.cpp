#include "ExecProbe.h"

#include <stackfull/exec/execution.hpp>

#include <lexec/execution.hpp>

#include <stackfull/io/Async.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/SocketAddress.h>
#include <stackfull/io/TcpStream.h>
#include <stackfull/io/UdpSocket.h>
#include <stackfull/io/UnixStream.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <vector>

#include <unistd.h>

namespace ex = lexec;
namespace exec = stackfull::exec;
namespace io = stackfull::io;
using namespace stackfull::sched;
using exec_probe::probe;
using exec_probe::raceStopAgainst;
using exec_probe::record;
using exec_probe::waitFor;
using std::chrono::milliseconds;

namespace {

enum class Backend { Epoll, Poll };

struct ExecIo : ::testing::TestWithParam<Backend> {
    void SetUp() override {
#if defined(__linux__)
        poller = GetParam() == Backend::Epoll ? io::makeEpollPoller() : io::makePollPoller();
#else
        poller = io::makePollPoller();
#endif
        SchedulerOptions options;
        options.workers = 2;
        options.driver = poller.get();
        scheduler = makeScheduler(options);
    }
    void TearDown() override {
        scheduler.reset(); // workers gone before the poller
        poller.reset();
    }

    io::UnixStreamPair pair() {
        io::UnixStreamPair ends = io::UnixStream::pair(*poller);
        EXPECT_TRUE(ends);
        return ends;
    }

    std::unique_ptr<io::Poller> poller;
    std::unique_ptr<Scheduler> scheduler;
};

std::string backendName(::testing::TestParamInfo<Backend> const &info) {
    return info.param == Backend::Epoll ? "epoll" : "poll";
}

template <class Sndr>
auto valueOf(Sndr &&sndr) {
    auto result = ex::sync_wait(static_cast<Sndr &&>(sndr));
    EXPECT_TRUE(result.has_value());
    return std::move(std::get<0>(*result));
}

// Writes until the socket's send buffer is full.
std::size_t fill(io::StreamSocket &stream) {
    static char const chunk[4096] = {};
    std::size_t total = 0;
    for (;;) {
        io::IoAttempt const attempt =
            io::tryWrite(stream.events(), stream.events().writeEvents(), chunk, sizeof chunk);
        if (attempt.wouldBlock or attempt.error) {
            return total;
        }
        total += attempt.bytes;
    }
}

} // namespace

TEST_P(ExecIo, ReadWaitsForDataThatAWriteSends) {
    io::UnixStreamPair ends = pair();
    char buffer[16] = {};
    record log;
    auto op = ex::connect(exec::async_read(*ends.first, buffer, sizeof buffer), probe{&log});
    ex::start(op);
    EXPECT_EQ(log.completions(), 0);
    EXPECT_EQ(valueOf(exec::async_write_all(*ends.second, "hello", 5)), 5u);
    ASSERT_TRUE(waitFor([&] { return log.completions() != 0; })); // on the polling thread
    EXPECT_EQ(log.last.load(), 5);
    EXPECT_EQ(std::memcmp(buffer, "hello", 5), 0);
}

TEST_P(ExecIo, ReadExactlyCollectsPiecesAndStopsAtTheEnd) {
    io::UnixStreamPair ends = pair();
    char buffer[8] = {};
    std::thread writer([&] {
        for (char const c : std::string("abcdef")) {
            ends.second->writeAll(&c, 1);
            std::this_thread::sleep_for(milliseconds{1});
        }
        ends.second->shutdownWrite();
    });
    EXPECT_EQ(valueOf(exec::async_read_exactly(*ends.first, buffer, 4)), 4u);
    EXPECT_EQ(valueOf(exec::async_read_exactly(*ends.first, buffer + 4, 4)), 2u); // the stream ended
    writer.join();
    EXPECT_EQ(std::string(buffer, 6), "abcdef");
    EXPECT_EQ(valueOf(exec::async_read(*ends.first, buffer, sizeof buffer)), 0u);
}

TEST_P(ExecIo, WriteAllWaitsForRoomInAFullSocket) {
    io::UnixStreamPair ends = pair();
    std::size_t const buffered = fill(*ends.second);
    ASSERT_GT(buffered, 0u);
    std::vector<char> payload(256 * 1024, 'x');
    record log;
    auto op = ex::connect(exec::async_write_all(*ends.second, payload.data(), payload.size()), probe{&log});
    ex::start(op);
    EXPECT_EQ(log.completions(), 0);
    std::size_t drained = 0;
    char sink[65536];
    while (drained < buffered + payload.size()) {
        io::IoResult const got = ends.first->read(sink, sizeof sink);
        ASSERT_TRUE(got);
        ASSERT_NE(got.bytes, 0u);
        drained += got.bytes;
    }
    ASSERT_TRUE(waitFor([&] { return log.completions() != 0; }));
    EXPECT_EQ(log.values.load(), 1);
    EXPECT_EQ(log.last.load(), static_cast<long>(payload.size()));
}

TEST_P(ExecIo, AStopRequestTakesAPendingReadBackAndTheSocketStaysUsable) {
    io::UnixStreamPair ends = pair();
    char buffer[4] = {};
    record log;
    ex::inplace_stop_source source;
    auto op = ex::connect(exec::async_read(*ends.first, buffer, sizeof buffer), probe{&log, &source});
    ex::start(op);
    source.request_stop();
    EXPECT_EQ(log.stops.load(), 1);
    ASSERT_TRUE(ends.second->writeAll("ok", 2));
    EXPECT_EQ(valueOf(exec::async_read(*ends.first, buffer, sizeof buffer)), 2u);
    EXPECT_EQ(log.completions(), 1);
}

TEST_P(ExecIo, InsideATaskWaitParksOnTheRead) {
    io::UnixStreamPair ends = pair();
    exec::scheduler const sf{*scheduler};
    std::thread writer([&] {
        std::this_thread::sleep_for(milliseconds{10});
        ends.second->writeAll("x", 1);
    });
    auto const got = ex::sync_wait(exec::spawn(sf, [&] {
        char byte = 0;
        auto const bytes = exec::wait(exec::async_read(*ends.first, &byte, 1));
        return bytes and std::get<0>(*bytes) == 1 and byte == 'x' and this_task::isTask();
    }));
    writer.join();
    ASSERT_TRUE(got);
    EXPECT_TRUE(std::get<0>(*got));
}

TEST_P(ExecIo, ContinuesOnMovesWhatFollowsIntoATask) {
    io::UnixStreamPair ends = pair();
    exec::scheduler const sf{*scheduler};
    char byte = 0;
    std::thread writer([&] {
        std::this_thread::sleep_for(milliseconds{5});
        ends.second->writeAll("y", 1);
    });
    bool const inTask = valueOf(exec::async_read(*ends.first, &byte, 1) | ex::continues_on(sf) |
                                ex::then([](std::size_t) { return this_task::isTask(); }));
    writer.join();
    EXPECT_TRUE(inTask);
    EXPECT_EQ(byte, 'y');
}

TEST_P(ExecIo, AcceptAndConnectOverTcp) {
    io::TcpListenerResult bound = io::TcpListener::bind(*poller, io::SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    auto streams = ex::sync_wait(ex::when_all(exec::async_accept(*bound.listener),
                                              exec::async_connect(*poller, bound.listener->localAddress())));
    ASSERT_TRUE(streams);
    std::unique_ptr<io::TcpStream> &server = std::get<0>(*streams);
    std::unique_ptr<io::TcpStream> &client = std::get<1>(*streams);
    ASSERT_TRUE(server and client);
    EXPECT_EQ(valueOf(exec::async_write_all(*client, "ping", 4)), 4u);
    char buffer[4] = {};
    EXPECT_EQ(valueOf(exec::async_read_exactly(*server, buffer, 4)), 4u);
    EXPECT_EQ(std::string(buffer, 4), "ping");
}

TEST_P(ExecIo, ConnectReportsARefusedConnection) {
    io::SocketAddress closed;
    {
        io::TcpListenerResult bound = io::TcpListener::bind(*poller, io::SocketAddress::loopback(0));
        ASSERT_TRUE(bound);
        closed = bound.listener->localAddress();
    }
    record log;
    auto op = ex::connect(exec::async_connect(*poller, closed), probe{&log});
    ex::start(op);
    ASSERT_TRUE(waitFor([&] { return log.completions() != 0; }));
    EXPECT_EQ(log.errors.load(), 1);
}

// A connect that is still under way when the stream is registered must not
// take that registration's report for the end of the handshake: the stream
// it hands out works.
TEST_P(ExecIo, ConnectedStreamsWorkRightAway) {
    io::TcpListenerResult bound = io::TcpListener::bind(*poller, io::SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    for (int i = 0; i < 50; ++i) {
        auto streams = ex::sync_wait(ex::when_all(exec::async_accept(*bound.listener),
                                                  exec::async_connect(*poller, bound.listener->localAddress())));
        ASSERT_TRUE(streams);
        std::unique_ptr<io::TcpStream> &client = std::get<1>(*streams);
        char const byte = 'z';
        ASSERT_EQ(::send(client->fd(), &byte, 1, 0), 1) << std::strerror(errno);
        char got = 0;
        EXPECT_EQ(valueOf(exec::async_read(*std::get<0>(*streams), &got, 1)), 1u);
        EXPECT_EQ(got, 'z');
    }
}

TEST_P(ExecIo, AcceptAndConnectOverAUnixSocket) {
    io::SocketAddress address;
    std::string const name = "stackfull-exec-" + std::to_string(::getpid()) + "-" +
                             (GetParam() == Backend::Epoll ? "epoll" : "poll");
    ASSERT_TRUE(io::SocketAddress::unixAbstract(name.c_str(), address));
    io::UnixListenerResult bound = io::UnixListener::bind(*poller, address);
    ASSERT_TRUE(bound);
    auto streams = ex::sync_wait(
        ex::when_all(exec::async_accept(*bound.listener), exec::async_connect<io::UnixStream>(*poller, address)));
    ASSERT_TRUE(streams);
    EXPECT_EQ(valueOf(exec::async_write_all(*std::get<1>(*streams), "u", 1)), 1u);
    char got = 0;
    EXPECT_EQ(valueOf(exec::async_read(*std::get<0>(*streams), &got, 1)), 1u);
    EXPECT_EQ(got, 'u');
}

TEST_P(ExecIo, UdpSendToAndRecvFrom) {
    io::UdpSocketResult server = io::UdpSocket::bind(*poller, io::SocketAddress::loopback(0));
    io::UdpSocketResult client = io::UdpSocket::connect(*poller, server.socket->localAddress());
    ASSERT_TRUE(server and client);
    char buffer[16] = {};
    io::SocketAddress from;
    record log;
    auto op = ex::connect(exec::async_recv_from(*server.socket, buffer, sizeof buffer, from), probe{&log});
    ex::start(op);
    EXPECT_EQ(valueOf(exec::async_send(*client.socket, "dgram", 5)), 5u);
    ASSERT_TRUE(waitFor([&] { return log.completions() != 0; }));
    EXPECT_EQ(log.last.load(), 5);
    EXPECT_EQ(std::string(buffer, 5), "dgram");
    EXPECT_EQ(from.port(), client.socket->localAddress().port());

    EXPECT_EQ(valueOf(exec::async_send_to(*server.socket, "back", 4, from)), 4u);
    EXPECT_EQ(valueOf(exec::async_recv(*client.socket, buffer, sizeof buffer)), 4u);
    EXPECT_EQ(std::string(buffer, 4), "back");
}

// Whichever of the data and the stop request wins, the byte is read once:
// by the operation, or left in the socket for the next read.
TEST_P(ExecIo, StopRacingDataLosesNoByte) {
    io::UnixStreamPair ends = pair();
    char byte = 0;
    char next = 0;
    raceStopAgainst(
        "read", 2000, [&] { return exec::async_read(*ends.first, &byte, 1); },
        [&] {
            next = static_cast<char>(next + 1);
            ASSERT_TRUE(ends.second->writeAll(&next, 1));
        },
        [&](record const &log) {
            ASSERT_EQ(log.completions(), 1);
            if (log.values.load() == 1) {
                EXPECT_EQ(byte, next);
            } else {
                char left = 0;
                io::IoResult const got = ends.first->readFor(std::chrono::seconds{5}, &left, 1);
                ASSERT_TRUE(got);
                EXPECT_EQ(left, next);
            }
        },
        std::chrono::microseconds{15});
}

#if defined(__linux__)
INSTANTIATE_TEST_SUITE_P(Backends, ExecIo, ::testing::Values(Backend::Epoll, Backend::Poll), backendName);
#else
INSTANTIATE_TEST_SUITE_P(Backends, ExecIo, ::testing::Values(Backend::Poll), backendName);
#endif
