#include <stackfull/io/Poller.h>
#include <stackfull/io/Resolve.h>
#include <stackfull/io/SocketAddress.h>
#include <stackfull/io/StreamSocket.h>
#include <stackfull/io/TcpStream.h>
#include <stackfull/io/UdpSocket.h>
#include <stackfull/io/UnixStream.h>
#include <stackfull/runtime/Runtime.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/Sleep.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/WaitGroup.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <netdb.h>
#include <unistd.h>

using namespace stackfull;
using namespace stackfull::io;
using namespace stackfull::sched;
using stackfull::sync::WaitGroup;
using std::chrono::milliseconds;

namespace {

enum class Backend { Epoll, Poll };

struct NetTest : ::testing::TestWithParam<Backend> {
    void SetUp() override {
#if defined(__linux__)
        poller = GetParam() == Backend::Epoll ? makeEpollPoller() : makePollPoller();
#else
        poller = makePollPoller();
#endif
        SchedulerOptions options;
        options.workers = 3;
        options.driver = poller.get();
        scheduler = makeScheduler(options);
    }
    void TearDown() override {
        scheduler.reset(); // workers gone before the poller
        poller.reset();
    }

    // Runs `body` as a task and waits for it.
    template <class F>
    void inTask(F body) {
        WaitGroup done;
        done.add(1);
        ASSERT_TRUE(scheduler->spawn([&] {
            body();
            done.done();
        }));
        done.wait();
    }

    std::unique_ptr<Poller> poller;
    std::unique_ptr<Scheduler> scheduler;
};

std::string backendName(::testing::TestParamInfo<Backend> const &info) {
    return info.param == Backend::Epoll ? "epoll" : "poll";
}

// A socket path in the working directory, removed before and after.
struct TempSocketPath {
    std::string path = "stackfull-net-test-" + std::to_string(::getpid()) + ".sock";
    TempSocketPath() { ::unlink(path.c_str()); }
    ~TempSocketPath() { ::unlink(path.c_str()); }
};

std::string readAll(StreamSocket &stream) {
    std::string text;
    char buffer[256];
    for (;;) {
        IoResult const got = stream.read(buffer, sizeof buffer);
        if (not got or got.bytes == 0) {
            return text;
        }
        text.append(buffer, got.bytes);
    }
}

} // namespace

// --- SocketAddress ------------------------------------------------------------------

TEST(SocketAddressTest, FormatsEachFamily) {
    EXPECT_EQ(SocketAddress::loopback(80).toString(), "127.0.0.1:80");
    EXPECT_EQ(SocketAddress::loopbackV6(443).toString(), "[::1]:443");
    SocketAddress path;
    ASSERT_TRUE(SocketAddress::unixPath("/run/app.sock", path));
    EXPECT_EQ(path.toString(), "unix:/run/app.sock");
    EXPECT_EQ(path.port(), 0);
#if defined(__linux__)
    SocketAddress abstract;
    ASSERT_TRUE(SocketAddress::unixAbstract("app", abstract));
    EXPECT_EQ(abstract.toString(), "unix:@app");
#endif
    SocketAddress tooLong;
    EXPECT_FALSE(SocketAddress::unixPath(std::string(200, 'x').c_str(), tooLong));
    SocketAddress parsed;
    ASSERT_TRUE(SocketAddress::parse("10.1.2.3", 8080, parsed));
    EXPECT_EQ(parsed.toString(), "10.1.2.3:8080");
    EXPECT_FALSE(SocketAddress::parse("not-an-address", 1, parsed));
}

// --- Unix-domain streams -------------------------------------------------------------

TEST_P(NetTest, UnixStreamPairCarriesBytesBothWays) {
    UnixStreamPair ends = UnixStream::pair(*poller);
    ASSERT_TRUE(ends) << ends.error.message();
    std::string heard;
    std::string answered;
    WaitGroup done;
    done.add(2);
    ASSERT_TRUE(scheduler->spawn([&] {
        char buffer[5];
        IoResult const got = ends.second->readExactly(buffer, sizeof buffer);
        heard.assign(buffer, got.bytes);
        ends.second->writeAll("world", 5);
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        ends.first->writeAll("hello", 5);
        char buffer[5];
        IoResult const got = ends.first->readExactly(buffer, sizeof buffer);
        answered.assign(buffer, got.bytes);
        done.done();
    }));
    done.wait();
    EXPECT_EQ(heard, "hello");
    EXPECT_EQ(answered, "world");
}

TEST_P(NetTest, UnixListenerAcceptsByPath) {
    TempSocketPath const temp;
    SocketAddress address;
    ASSERT_TRUE(SocketAddress::unixPath(temp.path.c_str(), address));
    UnixListenerResult bound = UnixListener::bind(*poller, address);
    ASSERT_TRUE(bound) << bound.error.message();
    std::string received;
    WaitGroup done;
    done.add(2);
    ASSERT_TRUE(scheduler->spawn([&] {
        UnixStreamResult peer = bound.listener->accept();
        EXPECT_TRUE(peer) << peer.error.message();
        if (peer) {
            received = readAll(*peer.stream);
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        UnixStreamResult client = UnixStream::connect(*poller, address);
        EXPECT_TRUE(client) << client.error.message();
        if (client) {
            client.stream->writeAll("over a path", 11);
        }
        done.done(); // dropping the client closes it: the peer sees EOF
    }));
    done.wait();
    EXPECT_EQ(received, "over a path");
}

#if defined(__linux__)
TEST_P(NetTest, UnixListenerAcceptsByAbstractName) {
    std::string const name = "stackfull-net-test-" + std::to_string(::getpid());
    SocketAddress address;
    ASSERT_TRUE(SocketAddress::unixAbstract(name.c_str(), address));
    UnixListenerResult bound = UnixListener::bind(*poller, address);
    ASSERT_TRUE(bound) << bound.error.message();
    std::string received;
    WaitGroup done;
    done.add(2);
    ASSERT_TRUE(scheduler->spawn([&] {
        UnixStreamResult peer = bound.listener->accept();
        EXPECT_TRUE(peer);
        if (peer) {
            received = readAll(*peer.stream);
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        UnixStreamResult client = UnixStream::connect(*poller, address);
        EXPECT_TRUE(client) << client.error.message();
        if (client) {
            client.stream->writeAll("abstract", 8);
        }
        done.done();
    }));
    done.wait();
    EXPECT_EQ(received, "abstract");
}

// A non-blocking connect to a full Unix-domain backlog fails with EAGAIN:
// no connection was queued, so it must not be reported as connected.
TEST_P(NetTest, UnixConnectToAFullBacklogFailsInsteadOfPretending) {
    std::string const name = "stackfull-backlog-" + std::to_string(::getpid());
    SocketAddress address;
    ASSERT_TRUE(SocketAddress::unixAbstract(name.c_str(), address));
    UnixListenerResult bound = UnixListener::bind(*poller, address, 1);
    ASSERT_TRUE(bound);
    inTask([&] {
        std::vector<std::unique_ptr<UnixStream>> queued;
        std::error_code refused;
        for (int i = 0; i < 16 and not refused; ++i) {
            UnixStreamResult client = UnixStream::connect(*poller, address);
            if (client) {
                queued.push_back(std::move(client.stream));
            } else {
                refused = client.error;
            }
        }
        EXPECT_EQ(refused, std::errc::resource_unavailable_try_again) << refused.message();
        EXPECT_LT(queued.size(), 16u);
    });
}
#endif

TEST_P(NetTest, UnixConnectToAMissingPathFails) {
    SocketAddress address;
    ASSERT_TRUE(SocketAddress::unixPath("stackfull-no-such-socket", address));
    inTask([&] {
        UnixStreamResult client = UnixStream::connect(*poller, address);
        EXPECT_FALSE(client);
        EXPECT_TRUE(client.error == std::errc::no_such_file_or_directory or
                    client.error == std::errc::connection_refused)
            << client.error.message();
    });
}

// --- BufReader -----------------------------------------------------------------------

TEST_P(NetTest, BufReaderSplitsLinesAcrossRefills) {
    UnixStreamPair ends = UnixStream::pair(*poller);
    ASSERT_TRUE(ends);
    std::vector<std::string> lines;
    inTask([&] {
        ends.first->writeAll("alpha\nbeta\ngam", 14);
        ends.first->shutdownWrite();
        BufReader reader(*ends.second, 4); // smaller than a line
        for (;;) {
            std::string line;
            IoResult const got = reader.readLine(line);
            EXPECT_TRUE(got) << got.error.message();
            if (got.bytes == 0) {
                break;
            }
            EXPECT_EQ(got.bytes, line.size());
            lines.push_back(line);
        }
    });
    EXPECT_EQ(lines, (std::vector<std::string>{"alpha\n", "beta\n", "gam"}));
}

TEST_P(NetTest, BufReaderStopsALineAtTheLimit) {
    UnixStreamPair ends = UnixStream::pair(*poller);
    ASSERT_TRUE(ends);
    inTask([&] {
        ends.first->writeAll("0123456789\nnext\n", 16);
        BufReader reader(*ends.second, 8);
        std::string line;
        IoResult const tooLong = reader.readLine(line, '\n', 4);
        EXPECT_EQ(tooLong.error, std::errc::message_size);
        EXPECT_EQ(line, "0123");
        line.clear();
        EXPECT_TRUE(reader.readLine(line)); // the rest of that line
        EXPECT_EQ(line, "456789\n");
        line.clear();
        EXPECT_TRUE(reader.readLine(line));
        EXPECT_EQ(line, "next\n");
    });
}

TEST_P(NetTest, BufReaderReadsLengthPrefixedFrames) {
    UnixStreamPair ends = UnixStream::pair(*poller);
    ASSERT_TRUE(ends);
    std::string const big(10000, 'z');
    std::vector<std::string> frames;
    WaitGroup done;
    done.add(2);
    ASSERT_TRUE(scheduler->spawn([&] {
        BufWriter out(*ends.first, 64);
        for (std::string const &payload : {std::string("hello"), std::string("abc"), big}) {
            std::uint32_t const length = static_cast<std::uint32_t>(payload.size());
            out.write(&length, sizeof length);
            out.write(payload.data(), payload.size());
        }
        out.flush();
        ends.first->shutdownWrite();
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        BufReader in(*ends.second, 64);
        for (;;) {
            std::uint32_t length = 0;
            if (in.readExactly(&length, sizeof length).bytes != sizeof length) {
                break;
            }
            std::string payload(length, '\0');
            EXPECT_EQ(in.readExactly(&payload[0], length).bytes, length);
            frames.push_back(payload);
        }
        done.done();
    }));
    done.wait();
    EXPECT_EQ(frames, (std::vector<std::string>{"hello", "abc", big}));
}

TEST_P(NetTest, BufReaderHonoursTheDeadline) {
    UnixStreamPair ends = UnixStream::pair(*poller);
    ASSERT_TRUE(ends);
    inTask([&] {
        BufReader reader(*ends.second);
        std::string line;
        auto const start = std::chrono::steady_clock::now();
        IoResult const got = reader.readLine(line, '\n', 1024, start + milliseconds{20});
        EXPECT_EQ(got.error, std::errc::timed_out);
        EXPECT_GE(std::chrono::steady_clock::now() - start, milliseconds{20});
    });
}

// --- TCP additions -------------------------------------------------------------------

TEST_P(NetTest, TcpStreamKnowsBothEnds) {
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    inTask([&] {
        TcpStreamResult client = TcpStream::connect(*poller, SocketAddress::loopback(bound.listener->port()));
        ASSERT_TRUE(client);
        TcpStreamResult peer = bound.listener->accept();
        ASSERT_TRUE(peer);
        EXPECT_EQ(client.stream->peerAddress().port(), bound.listener->port());
        EXPECT_EQ(client.stream->localAddress().toString(), peer.stream->peerAddress().toString());
    });
}

TEST_P(NetTest, TcpConnectFallsThroughTheAddressList) {
    TcpListenerResult bound = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    // Nothing listens on the first one (a port just released).
    TcpListenerResult closed = TcpListener::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(closed);
    SocketAddress const dead = closed.listener->localAddress();
    closed.listener.reset();
    inTask([&] {
        std::vector<SocketAddress> const addresses = {dead, bound.listener->localAddress()};
        TcpStreamResult client = TcpStream::connect(*poller, addresses);
        EXPECT_TRUE(client) << client.error.message();
        if (client) {
            EXPECT_EQ(client.stream->peerAddress().port(), bound.listener->port());
        }
        EXPECT_FALSE(TcpStream::connect(*poller, std::vector<SocketAddress>{dead}));
        EXPECT_FALSE(TcpStream::connect(*poller, std::vector<SocketAddress>{}));
    });
}

#if defined(SO_REUSEPORT)
TEST_P(NetTest, TcpListenersShareAPortWithReusePort) {
    TcpListenOptions options;
    options.reusePort = true;
    TcpListenerResult first = TcpListener::bind(*poller, SocketAddress::loopback(0), options);
    ASSERT_TRUE(first) << first.error.message();
    TcpListenerResult second = TcpListener::bind(*poller, first.listener->localAddress(), options);
    EXPECT_TRUE(second) << second.error.message();
}
#endif

// --- UDP -----------------------------------------------------------------------------

TEST_P(NetTest, UdpRequestAndReply) {
    UdpSocketResult server = UdpSocket::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(server) << server.error.message();
    SocketAddress const serverAddress = server.socket->localAddress();
    std::string request;
    std::string reply;
    std::string clientSeenAs;
    std::string clientIs;
    WaitGroup done;
    done.add(2);
    ASSERT_TRUE(scheduler->spawn([&] {
        char buffer[64];
        SocketAddress from;
        IoResult const got = server.socket->recvFrom(buffer, sizeof buffer, from);
        EXPECT_TRUE(got);
        request.assign(buffer, got.bytes);
        clientSeenAs = from.toString();
        EXPECT_TRUE(server.socket->sendTo("pong", 4, from));
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        UdpSocketResult client = UdpSocket::connect(*poller, serverAddress);
        EXPECT_TRUE(client) << client.error.message();
        if (client) {
            clientIs = client.socket->localAddress().toString();
            EXPECT_TRUE(client.socket->send("ping", 4));
            char buffer[64];
            IoResult const got = client.socket->recvFor(std::chrono::seconds{5}, buffer, sizeof buffer);
            EXPECT_TRUE(got) << got.error.message();
            reply.assign(buffer, got.bytes);
        }
        done.done();
    }));
    done.wait();
    EXPECT_EQ(request, "ping");
    EXPECT_EQ(reply, "pong");
    EXPECT_EQ(clientSeenAs, clientIs);
}

TEST_P(NetTest, UdpRecvForTimesOut) {
    UdpSocketResult socket = UdpSocket::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(socket);
    inTask([&] {
        char buffer[16];
        auto const start = std::chrono::steady_clock::now();
        IoResult const got = socket.socket->recvFor(milliseconds{20}, buffer, sizeof buffer);
        EXPECT_EQ(got.error, std::errc::timed_out);
        EXPECT_GE(std::chrono::steady_clock::now() - start, milliseconds{20});
    });
}

TEST_P(NetTest, UdpBatchesSendAndReceiveMany) {
    UdpSocketResult receiver = UdpSocket::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(receiver);
    SocketAddress const to = receiver.socket->localAddress();
    // More than one kernel batch (64), few enough not to overrun the
    // receive buffer if the sender gets ahead.
    constexpr std::size_t kCount = 100;
    std::set<std::uint32_t> seen;
    bool allFromSender = true;
    std::string senderIs;
    WaitGroup done;
    done.add(2);
    ASSERT_TRUE(scheduler->spawn([&] {
        std::vector<std::uint32_t> buffers(kCount);
        std::vector<Datagram> datagrams(kCount);
        std::vector<std::string> from;
        while (seen.size() < kCount) {
            for (std::size_t i = 0; i < kCount; ++i) {
                datagrams[i].data = &buffers[i];
                datagrams[i].capacity = sizeof buffers[i];
            }
            BatchResult const got = receiver.socket->recvManyUntil(
                std::chrono::steady_clock::now() + std::chrono::seconds{5}, datagrams.data(), kCount);
            if (not got) {
                ADD_FAILURE() << got.error.message();
                break;
            }
            for (std::size_t i = 0; i < got.count; ++i) {
                EXPECT_EQ(datagrams[i].size, sizeof(std::uint32_t));
                EXPECT_FALSE(datagrams[i].truncated);
                seen.insert(buffers[i]);
                from.push_back(datagrams[i].address.toString());
            }
        }
        while (senderIs.empty()) {
            this_task::sleepFor(milliseconds{1});
        }
        for (std::string const &address : from) {
            allFromSender = allFromSender and address == senderIs;
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        UdpSocketResult sender = UdpSocket::bind(*poller, SocketAddress::loopback(0));
        EXPECT_TRUE(sender);
        if (sender) {
            senderIs = sender.socket->localAddress().toString();
            std::vector<std::uint32_t> payloads(kCount);
            std::vector<Datagram> datagrams(kCount);
            for (std::size_t i = 0; i < kCount; ++i) {
                payloads[i] = static_cast<std::uint32_t>(i);
                datagrams[i].data = &payloads[i];
                datagrams[i].size = sizeof payloads[i];
                datagrams[i].address = to;
            }
            std::size_t sent = 0;
            while (sent < kCount) {
                BatchResult const batch = sender.socket->sendMany(datagrams.data() + sent, kCount - sent);
                if (not batch) {
                    ADD_FAILURE() << batch.error.message();
                    break;
                }
                sent += batch.count;
            }
        }
        done.done();
    }));
    done.wait();
    EXPECT_EQ(seen.size(), kCount);
    EXPECT_TRUE(allFromSender);
}

TEST_P(NetTest, UdpBatchReportsTruncation) {
    UdpSocketResult receiver = UdpSocket::bind(*poller, SocketAddress::loopback(0));
    ASSERT_TRUE(receiver);
    inTask([&] {
        UdpSocketResult sender = UdpSocket::connect(*poller, receiver.socket->localAddress());
        ASSERT_TRUE(sender);
        std::string const payload(100, 'q');
        ASSERT_TRUE(sender.socket->send(payload.data(), payload.size()));
        char buffer[10];
        Datagram datagram;
        datagram.data = buffer;
        datagram.capacity = sizeof buffer;
        BatchResult const got = receiver.socket->recvMany(&datagram, 1);
        ASSERT_TRUE(got);
        EXPECT_EQ(got.count, 1u);
        EXPECT_EQ(datagram.size, sizeof buffer);
        EXPECT_TRUE(datagram.truncated);
    });
}

// --- Name resolution -----------------------------------------------------------------

TEST(ResolveTest, NumericHostsNeedNoLookup) {
    ResolveResult const v4 = io::resolve("127.0.0.1", 80);
    ASSERT_TRUE(v4);
    ASSERT_EQ(v4.addresses.size(), 1u);
    EXPECT_EQ(v4.addresses[0].toString(), "127.0.0.1:80");
    ResolveResult const v6 = io::resolve("::1", 443);
    ASSERT_TRUE(v6);
    EXPECT_EQ(v6.addresses[0].toString(), "[::1]:443");
    ResolveResult const wrongFamily = io::resolve("127.0.0.1", 80, AF_INET6);
    EXPECT_EQ(wrongFamily.error, std::error_code(EAI_FAMILY, resolveCategory()));
}

TEST(ResolveTest, LocalhostResolvesToLoopback) {
    ResolveResult const local = io::resolve("localhost", 8080);
    ASSERT_TRUE(local) << local.error.message();
    ASSERT_FALSE(local.addresses.empty());
    for (SocketAddress const &address : local.addresses) {
        EXPECT_EQ(address.port(), 8080);
        std::string const text = address.toString();
        EXPECT_TRUE(text == "127.0.0.1:8080" or text == "[::1]:8080") << text;
    }
}

TEST(ResolveTest, UnknownNamesFail) {
    ResolveResult const missing = io::resolve("no-such-host.invalid", 80);
    EXPECT_FALSE(missing);
    EXPECT_TRUE(missing.addresses.empty());
    EXPECT_FALSE(missing.error.message().empty());
}

// runtime: the lookup goes to the blocking pool, then the address list is
// tried in order ("localhost" may come back as ::1 first; the listener is
// IPv4 only).
TEST(ResolveTest, ConnectTcpByName) {
    TcpListenerResult bound = TcpListener::bind(defaultPoller(), SocketAddress::loopback(0));
    ASSERT_TRUE(bound);
    std::uint16_t const port = bound.listener->port();
    std::string greeting = blockOn([&] {
        auto accepted = async([&] {
            TcpStreamResult peer = bound.listener->accept();
            return peer ? readAll(*peer.stream) : std::string("accept failed");
        });
        TcpStreamResult client = connectTcp("localhost", port, std::chrono::steady_clock::now() + std::chrono::seconds{5});
        EXPECT_TRUE(client) << client.error.message();
        if (client) {
            client.stream->writeAll("by name", 7);
            client.stream.reset();
        }
        ResolveResult const numeric = stackfull::resolve("127.0.0.1", port);
        EXPECT_TRUE(numeric);
        return accepted.get().value;
    });
    EXPECT_EQ(greeting, "by name");
}

#if defined(__linux__)
INSTANTIATE_TEST_SUITE_P(Backends, NetTest, ::testing::Values(Backend::Epoll, Backend::Poll), backendName);
#else
INSTANTIATE_TEST_SUITE_P(Backends, NetTest, ::testing::Values(Backend::Poll), backendName);
#endif
