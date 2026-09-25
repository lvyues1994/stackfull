#include "Socket.h"

#include <utility>

namespace stackfull {
namespace io {
namespace detail {

std::error_code openSocket(int const family, int const type, Fd &out) noexcept {
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    Fd socket{::socket(family, type | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    if (not socket) {
        return lastError();
    }
#else
    Fd socket{::socket(family, type, 0)};
    if (not socket) {
        return lastError();
    }
    if (std::error_code const error = setNonBlocking(socket.get())) {
        return error;
    }
#endif
    suppressSigPipe(socket.get());
    out = std::move(socket);
    return std::error_code{};
}

void suppressSigPipe(int const fd) noexcept {
#if !defined(MSG_NOSIGNAL) && defined(SO_NOSIGPIPE)
    int const one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#else
    static_cast<void>(fd);
#endif
}

std::error_code bindAndListen(int const fd, SocketAddress const &address, int const backlog) noexcept {
    if (::bind(fd, address.get(), address.length) != 0 or ::listen(fd, backlog) != 0) {
        return lastError();
    }
    return std::error_code{};
}

SocketAddress localAddressOf(int const fd) noexcept {
    SocketAddress address;
    address.length = sizeof address.storage;
    if (::getsockname(fd, reinterpret_cast<sockaddr *>(&address.storage), &address.length) != 0) {
        return SocketAddress{};
    }
    return address;
}

SocketAddress peerAddressOf(int const fd) noexcept {
    SocketAddress address;
    address.length = sizeof address.storage;
    if (::getpeername(fd, reinterpret_cast<sockaddr *>(&address.storage), &address.length) != 0) {
        return SocketAddress{};
    }
    return address;
}

} // namespace detail
} // namespace io
} // namespace stackfull
