#include <stackfull/io/UdpSocket.h>

#include "Socket.h"

#include <cerrno>
#include <cstddef>

#include <sys/socket.h>
#include <sys/uio.h>

namespace stackfull {
namespace io {

namespace {

using detail::kNoSigPipe;

// Batches go to the kernel in pieces of this many (headers on the stack);
// one at a time without recvmmsg/sendmmsg.
constexpr std::size_t kBatch = 64;
#if defined(__linux__)
constexpr std::size_t kPiece = kBatch;
#else
constexpr std::size_t kPiece = 1;
#endif

std::size_t pieceOf(std::size_t const remaining) noexcept {
    return remaining < kPiece ? remaining : kPiece;
}

void describeReceive(Datagram &datagram, iovec &vector, msghdr &header) noexcept {
    vector = iovec{datagram.data, datagram.capacity};
    header = msghdr{};
    header.msg_name = &datagram.address.storage;
    header.msg_namelen = sizeof datagram.address.storage;
    header.msg_iov = &vector;
    header.msg_iovlen = 1;
}

void recordReceive(Datagram &datagram, msghdr const &header, std::size_t const bytes) noexcept {
    datagram.size = bytes;
    datagram.truncated = (header.msg_flags & MSG_TRUNC) != 0;
    datagram.address.length = header.msg_namelen;
}

void describeSend(Datagram const &datagram, iovec &vector, msghdr &header) noexcept {
    vector = iovec{datagram.data, datagram.size};
    header = msghdr{};
    if (datagram.address.length != 0) {
        header.msg_name = const_cast<sockaddr_storage *>(&datagram.address.storage);
        header.msg_namelen = datagram.address.length;
    }
    header.msg_iov = &vector;
    header.msg_iovlen = 1;
}

// One piece: at most kBatch datagrams in one system call where there is
// recvmmsg/sendmmsg, else one. Returns the count, or -1 with errno.
ssize_t receivePiece(int const fd, Datagram *const datagrams, std::size_t const count) noexcept {
    std::size_t const piece = pieceOf(count);
#if defined(__linux__)
    iovec vectors[kBatch];
    mmsghdr headers[kBatch];
    for (std::size_t i = 0; i < piece; ++i) {
        describeReceive(datagrams[i], vectors[i], headers[i].msg_hdr);
        headers[i].msg_len = 0;
    }
    int const n = ::recvmmsg(fd, headers, static_cast<unsigned>(piece), 0, nullptr);
    for (int i = 0; i < n; ++i) {
        recordReceive(datagrams[i], headers[i].msg_hdr, headers[i].msg_len);
    }
    return n;
#else
    static_cast<void>(piece);
    iovec vector;
    msghdr header;
    describeReceive(datagrams[0], vector, header);
    ssize_t const n = ::recvmsg(fd, &header, 0);
    if (n < 0) {
        return -1;
    }
    recordReceive(datagrams[0], header, static_cast<std::size_t>(n));
    return 1;
#endif
}

ssize_t sendPiece(int const fd, Datagram const *const datagrams, std::size_t const count) noexcept {
    std::size_t const piece = pieceOf(count);
#if defined(__linux__)
    iovec vectors[kBatch];
    mmsghdr headers[kBatch];
    for (std::size_t i = 0; i < piece; ++i) {
        describeSend(datagrams[i], vectors[i], headers[i].msg_hdr);
        headers[i].msg_len = 0;
    }
    return ::sendmmsg(fd, headers, static_cast<unsigned>(piece), kNoSigPipe);
#else
    static_cast<void>(piece);
    iovec vector;
    msghdr header;
    describeSend(datagrams[0], vector, header);
    return ::sendmsg(fd, &header, kNoSigPipe) < 0 ? -1 : 1;
#endif
}

} // namespace

UdpSocketResult UdpSocket::open(Poller &poller, int const family) {
    Fd socket;
    if (std::error_code const error = detail::openSocket(family, SOCK_DGRAM, socket)) {
        return UdpSocketResult{nullptr, error};
    }
    std::unique_ptr<UdpSocket> udp(new UdpSocket(poller, std::move(socket)));
    if (std::error_code const error = udp->registration.error()) {
        return UdpSocketResult{nullptr, error};
    }
    return UdpSocketResult{std::move(udp), std::error_code{}};
}

UdpSocketResult UdpSocket::bind(Poller &poller, SocketAddress const &local) {
    UdpSocketResult result = open(poller, local.family());
    if (result and ::bind(result.socket->fd(), local.get(), local.length) != 0) {
        return UdpSocketResult{nullptr, lastError()};
    }
    return result;
}

UdpSocketResult UdpSocket::connect(Poller &poller, SocketAddress const &peer) {
    UdpSocketResult result = open(poller, peer.family());
    if (result and ::connect(result.socket->fd(), peer.get(), peer.length) != 0) {
        return UdpSocketResult{nullptr, lastError()};
    }
    return result;
}

IoResult UdpSocket::send(void const *const data, std::size_t const length) {
    int const descriptor = socket.get();
    return callWhenReady(registration, Interest::Writable, length, nullptr,
                         [&] { return ::send(descriptor, data, length, kNoSigPipe); });
}

IoResult UdpSocket::sendTo(void const *const data, std::size_t const length, SocketAddress const &to) {
    int const descriptor = socket.get();
    return callWhenReady(registration, Interest::Writable, length, nullptr,
                         [&] { return ::sendto(descriptor, data, length, kNoSigPipe, to.get(), to.length); });
}

IoResult UdpSocket::recvImpl(void *const buffer, std::size_t const length, SocketAddress *const from,
                             Deadline const *const deadline) {
    int const descriptor = socket.get();
    if (from == nullptr) {
        return callWhenReady(registration, Interest::Readable, length, deadline,
                             [&] { return ::recv(descriptor, buffer, length, 0); });
    }
    return callWhenReady(registration, Interest::Readable, length, deadline, [&] {
        socklen_t addressLength = sizeof from->storage;
        ssize_t const n =
            ::recvfrom(descriptor, buffer, length, 0, reinterpret_cast<sockaddr *>(&from->storage), &addressLength);
        if (n >= 0) {
            from->length = addressLength;
        }
        return n;
    });
}

BatchResult UdpSocket::recvManyImpl(Datagram *const datagrams, std::size_t const count, Deadline const *const deadline) {
    if (count == 0) {
        return BatchResult{0, std::error_code{}};
    }
    int const descriptor = socket.get();
    IoResult const first = callWhenReady(registration, Interest::Readable, 0, deadline,
                                         [&] { return receivePiece(descriptor, datagrams, count); });
    if (not first) {
        return BatchResult{0, first.error};
    }
    std::size_t received = first.bytes;
    std::size_t lastAsked = pieceOf(count);
    std::size_t lastGot = first.bytes;
    // A full piece: more may be queued. Take it without waiting.
    while (received < count and lastGot == lastAsked) {
        lastAsked = pieceOf(count - received);
        ssize_t const n = receivePiece(descriptor, datagrams + received, count - received);
        if (n <= 0) {
            break; // EAGAIN (or an error the next call will report)
        }
        lastGot = static_cast<std::size_t>(n);
        received += lastGot;
    }
    return BatchResult{received, std::error_code{}};
}

BatchResult UdpSocket::sendMany(Datagram const *const datagrams, std::size_t const count) {
    if (count == 0) {
        return BatchResult{0, std::error_code{}};
    }
    int const descriptor = socket.get();
    IoResult const first = callWhenReady(registration, Interest::Writable, 0, nullptr,
                                         [&] { return sendPiece(descriptor, datagrams, count); });
    if (not first) {
        return BatchResult{0, first.error};
    }
    std::size_t sent = first.bytes;
    std::size_t lastAsked = pieceOf(count);
    std::size_t lastSent = first.bytes;
    while (sent < count and lastSent == lastAsked) {
        lastAsked = pieceOf(count - sent);
        ssize_t const n = sendPiece(descriptor, datagrams + sent, count - sent);
        if (n <= 0) {
            break; // no room (or an error the next call will report)
        }
        lastSent = static_cast<std::size_t>(n);
        sent += lastSent;
    }
    return BatchResult{sent, std::error_code{}};
}

std::error_code UdpSocket::setBroadcast(bool const enabled) noexcept {
    int const value = enabled ? 1 : 0;
    if (::setsockopt(socket.get(), SOL_SOCKET, SO_BROADCAST, &value, sizeof value) != 0) {
        return lastError();
    }
    return std::error_code{};
}

SocketAddress UdpSocket::localAddress() const noexcept {
    return detail::localAddressOf(socket.get());
}

} // namespace io
} // namespace stackfull
