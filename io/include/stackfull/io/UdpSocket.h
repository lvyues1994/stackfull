#pragma once

#include <stackfull/io/Async.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>
#include <stackfull/io/SocketAddress.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <system_error>
#include <utility>

namespace stackfull {
namespace io {

// One datagram for UdpSocket::recvMany / sendMany.
//   receive: data and capacity in; size, truncated and the sender out.
//   send:    data, size and the destination in (an address of length 0
//            sends to the connected peer).
struct Datagram {
    void *data = nullptr;
    std::size_t capacity = 0;
    std::size_t size = 0;
    bool truncated = false; // it was longer than capacity; the rest is lost
    SocketAddress address;
};

struct BatchResult {
    std::size_t count = 0; // datagrams received or sent
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

struct UdpSocket;

struct UdpSocketResult {
    std::unique_ptr<UdpSocket> socket;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// A UDP socket registered with a Poller. Every call works from a task
// (parks) or from a plain thread (blocks). A receive into a buffer shorter
// than the datagram keeps the start and drops the rest (recvMany reports
// it); a send is all or nothing.
struct UdpSocket {
    // Bound to `local` (port 0 picks one; localAddress() tells). The shape
    // of a server: recvFrom() / sendTo().
    static UdpSocketResult bind(Poller &poller, SocketAddress const &local);
    // Bound to an ephemeral port and connected to `peer`: send() / recv(),
    // and datagrams from anyone else are filtered out by the kernel.
    static UdpSocketResult connect(Poller &poller, SocketAddress const &peer);

    UdpSocket(UdpSocket const &) = delete;
    UdpSocket &operator=(UdpSocket const &) = delete;
    ~UdpSocket() = default;

    IoResult send(void const *data, std::size_t length);
    IoResult sendTo(void const *data, std::size_t length, SocketAddress const &to);

    IoResult recv(void *buffer, std::size_t length) { return recvImpl(buffer, length, nullptr, nullptr); }
    IoResult recvUntil(Deadline const deadline, void *buffer, std::size_t length) {
        return recvImpl(buffer, length, nullptr, &deadline);
    }
    template <class Rep, class Period>
    IoResult recvFor(std::chrono::duration<Rep, Period> const timeout, void *buffer, std::size_t length) {
        return recvUntil(deadlineAfter(timeout), buffer, length);
    }

    IoResult recvFrom(void *buffer, std::size_t length, SocketAddress &from) {
        return recvImpl(buffer, length, &from, nullptr);
    }
    IoResult recvFromUntil(Deadline const deadline, void *buffer, std::size_t length, SocketAddress &from) {
        return recvImpl(buffer, length, &from, &deadline);
    }
    template <class Rep, class Period>
    IoResult recvFromFor(std::chrono::duration<Rep, Period> const timeout, void *buffer, std::size_t length,
                         SocketAddress &from) {
        return recvFromUntil(deadlineAfter(timeout), buffer, length, from);
    }

    // Waits for at least one datagram, then takes as many as are queued, up
    // to `count`, in one system call where the platform has recvmmsg.
    BatchResult recvMany(Datagram *datagrams, std::size_t count) { return recvManyImpl(datagrams, count, nullptr); }
    BatchResult recvManyUntil(Deadline const deadline, Datagram *datagrams, std::size_t count) {
        return recvManyImpl(datagrams, count, &deadline);
    }
    // Sends as many as the kernel takes (at least one, waiting for room if
    // needed), in one system call where the platform has sendmmsg. Call
    // again with the rest.
    BatchResult sendMany(Datagram const *datagrams, std::size_t count);

    std::error_code setBroadcast(bool enabled) noexcept;
    SocketAddress localAddress() const noexcept;
    int fd() const noexcept { return socket.get(); }
    Registration &events() noexcept { return registration; }

private:
    UdpSocket(Poller &poller, Fd socket_) noexcept : socket(std::move(socket_)), registration(poller, socket.get()) {
        registration.setKind(Registration::Kind::DatagramSocket);
    }
    static UdpSocketResult open(Poller &poller, int family);
    template <class Rep, class Period>
    static Deadline deadlineAfter(std::chrono::duration<Rep, Period> const timeout) {
        return std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::nanoseconds>(timeout);
    }
    IoResult recvImpl(void *buffer, std::size_t length, SocketAddress *from, Deadline const *deadline);
    BatchResult recvManyImpl(Datagram *datagrams, std::size_t count, Deadline const *deadline);

    Fd socket;                 // declared first: closed last
    Registration registration; // leaves the poller before the socket closes
};

} // namespace io
} // namespace stackfull
