#include <stackfull/io/TcpStream.h>

#include <cstring>
#include <utility>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace stackfull {
namespace io {

// --- SocketAddress -------------------------------------------------------------

bool SocketAddress::parse(char const *const host, std::uint16_t const port, SocketAddress &out) noexcept {
    SocketAddress result;
    auto *const v4 = reinterpret_cast<sockaddr_in *>(&result.storage);
    if (::inet_pton(AF_INET, host, &v4->sin_addr) == 1) {
        v4->sin_family = AF_INET;
        v4->sin_port = htons(port);
        result.length = sizeof(sockaddr_in);
        out = result;
        return true;
    }
    auto *const v6 = reinterpret_cast<sockaddr_in6 *>(&result.storage);
    if (::inet_pton(AF_INET6, host, &v6->sin6_addr) == 1) {
        v6->sin6_family = AF_INET6;
        v6->sin6_port = htons(port);
        result.length = sizeof(sockaddr_in6);
        out = result;
        return true;
    }
    return false;
}

namespace {

SocketAddress ipv4(std::uint32_t const hostOrder, std::uint16_t const port) noexcept {
    SocketAddress result;
    auto *const v4 = reinterpret_cast<sockaddr_in *>(&result.storage);
    v4->sin_family = AF_INET;
    v4->sin_port = htons(port);
    v4->sin_addr.s_addr = htonl(hostOrder);
    result.length = sizeof(sockaddr_in);
    return result;
}

// Non-blocking, close-on-exec TCP socket for `family`.
std::error_code openSocket(int const family, Fd &out) noexcept {
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    Fd socket{::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    if (not socket) {
        return lastError();
    }
#else
    Fd socket{::socket(family, SOCK_STREAM, 0)};
    if (not socket) {
        return lastError();
    }
    if (std::error_code const error = setNonBlocking(socket.get())) {
        return error;
    }
#endif
    out = std::move(socket);
    return std::error_code{};
}

std::error_code enableNoDelay(int const fd, bool const enabled) noexcept {
    int const value = enabled ? 1 : 0;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, sizeof value) != 0) {
        return lastError();
    }
    return std::error_code{};
}

} // namespace

SocketAddress SocketAddress::loopback(std::uint16_t const port) noexcept {
    return ipv4(INADDR_LOOPBACK, port);
}

SocketAddress SocketAddress::any(std::uint16_t const port) noexcept {
    return ipv4(INADDR_ANY, port);
}

std::uint16_t SocketAddress::port() const noexcept {
    if (storage.ss_family == AF_INET) {
        return ntohs(reinterpret_cast<sockaddr_in const *>(&storage)->sin_port);
    }
    if (storage.ss_family == AF_INET6) {
        return ntohs(reinterpret_cast<sockaddr_in6 const *>(&storage)->sin6_port);
    }
    return 0;
}

// --- TcpStream -------------------------------------------------------------------

TcpStream::TcpStream(Fd socket_, std::unique_ptr<Registration> registration_) noexcept
    : socket(std::move(socket_)), registration(std::move(registration_)) {}

TcpStreamResult TcpStream::connect(Poller &poller, SocketAddress const &address, Deadline const deadline) {
    Fd socket;
    if (std::error_code const error = openSocket(address.family(), socket)) {
        return TcpStreamResult{nullptr, error};
    }
    enableNoDelay(socket.get(), true);
    auto registration = std::make_unique<Registration>(poller, socket.get());
    if (registration->error()) {
        return TcpStreamResult{nullptr, registration->error()};
    }
    std::error_code const error = deadline == Deadline::max()
                                      ? io::connect(*registration, address.get(), address.length)
                                      : io::connectUntil(*registration, address.get(), address.length, deadline);
    if (error) {
        registration.reset(); // before the socket closes
        return TcpStreamResult{nullptr, error};
    }
    return TcpStreamResult{std::unique_ptr<TcpStream>(new TcpStream(std::move(socket), std::move(registration))),
                           std::error_code{}};
}

TcpStreamResult TcpStream::adopt(Poller &poller, Fd socket) {
    if (std::error_code const error = setNonBlocking(socket.get())) {
        return TcpStreamResult{nullptr, error};
    }
    enableNoDelay(socket.get(), true);
    auto registration = std::make_unique<Registration>(poller, socket.get());
    if (registration->error()) {
        return TcpStreamResult{nullptr, registration->error()};
    }
    return TcpStreamResult{std::unique_ptr<TcpStream>(new TcpStream(std::move(socket), std::move(registration))),
                           std::error_code{}};
}

std::error_code TcpStream::setNoDelay(bool const enabled) noexcept {
    return enableNoDelay(socket.get(), enabled);
}

std::error_code TcpStream::shutdownWrite() noexcept {
    if (::shutdown(socket.get(), SHUT_WR) != 0) {
        return lastError();
    }
    return std::error_code{};
}

// --- TcpListener -----------------------------------------------------------------

TcpListener::TcpListener(Poller &poller_, Fd socket_, std::unique_ptr<Registration> registration_,
                         SocketAddress const &local_) noexcept
    : poller(poller_), socket(std::move(socket_)), registration(std::move(registration_)), local(local_) {}

TcpListenerResult TcpListener::bind(Poller &poller, SocketAddress const &address, int const backlog) {
    Fd socket;
    if (std::error_code const error = openSocket(address.family(), socket)) {
        return TcpListenerResult{nullptr, error};
    }
    int const one = 1;
    ::setsockopt(socket.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (::bind(socket.get(), address.get(), address.length) != 0 or ::listen(socket.get(), backlog) != 0) {
        return TcpListenerResult{nullptr, lastError()};
    }
    SocketAddress local;
    local.length = sizeof local.storage;
    if (::getsockname(socket.get(), reinterpret_cast<sockaddr *>(&local.storage), &local.length) != 0) {
        return TcpListenerResult{nullptr, lastError()};
    }
    auto registration = std::make_unique<Registration>(poller, socket.get());
    if (registration->error()) {
        return TcpListenerResult{nullptr, registration->error()};
    }
    return TcpListenerResult{
        std::unique_ptr<TcpListener>(new TcpListener(poller, std::move(socket), std::move(registration), local)),
        std::error_code{}};
}

TcpStreamResult TcpListener::acceptUntil(Deadline const deadline) {
    AcceptResult accepted =
        deadline == Deadline::max() ? io::accept(*registration) : io::acceptUntil(*registration, deadline);
    if (not accepted) {
        return TcpStreamResult{nullptr, accepted.error};
    }
    return TcpStream::adopt(poller, std::move(accepted.fd));
}

// --- BufWriter -------------------------------------------------------------------

BufWriter::BufWriter(TcpStream &stream_, std::size_t const capacity_)
    : stream(stream_), buffer(std::make_unique<char[]>(capacity_)), capacity(capacity_) {}

IoResult BufWriter::write(void const *const data, std::size_t const length) {
    if (used + length <= capacity) {
        std::memcpy(buffer.get() + used, data, length);
        used += length;
        return IoResult{length, std::error_code{}};
    }
    iovec vectors[2] = {{buffer.get(), used}, {const_cast<void *>(data), length}};
    IoResult const sent = stream.writevAll(vectors, 2);
    std::size_t const fromBuffer = sent.bytes < used ? sent.bytes : used;
    std::size_t const buffered = used;
    used = 0;
    if (not sent) {
        // Keep what the peer did not get from the buffer; report the payload unsent.
        std::memmove(buffer.get(), buffer.get() + fromBuffer, buffered - fromBuffer);
        used = buffered - fromBuffer;
        return IoResult{sent.bytes > buffered ? sent.bytes - buffered : 0, sent.error};
    }
    return IoResult{length, std::error_code{}};
}

IoResult BufWriter::flush() {
    if (used == 0) {
        return IoResult{0, std::error_code{}};
    }
    IoResult const sent = stream.writeAll(buffer.get(), used);
    std::memmove(buffer.get(), buffer.get() + sent.bytes, used - sent.bytes);
    used -= sent.bytes;
    return sent;
}

} // namespace io
} // namespace stackfull
