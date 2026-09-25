#include <stackfull/io/TcpStream.h>

#include "Socket.h"

#include <utility>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace stackfull {
namespace io {

namespace {

std::error_code enableNoDelay(int const fd, bool const enabled) noexcept {
    int const value = enabled ? 1 : 0;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, sizeof value) != 0) {
        return lastError();
    }
    return std::error_code{};
}

} // namespace

// --- TcpStream -------------------------------------------------------------------

TcpStreamResult TcpStream::wrap(Poller &poller, Fd socket) {
    detail::suppressSigPipe(socket.get());
    enableNoDelay(socket.get(), true);
    std::unique_ptr<TcpStream> stream(new TcpStream(poller, std::move(socket)));
    if (std::error_code const error = stream->events().error()) {
        return TcpStreamResult{nullptr, error};
    }
    return TcpStreamResult{std::move(stream), std::error_code{}};
}

TcpStreamResult TcpStream::connect(Poller &poller, SocketAddress const &address, Deadline const deadline) {
    Fd socket;
    if (std::error_code const error = detail::openSocket(address.family(), SOCK_STREAM, socket)) {
        return TcpStreamResult{nullptr, error};
    }
    TcpStreamResult result = wrap(poller, std::move(socket));
    if (not result) {
        return result;
    }
    Registration &registration = result.stream->events();
    std::error_code const error = deadline == Deadline::max()
                                      ? io::connect(registration, address.get(), address.length)
                                      : io::connectUntil(registration, address.get(), address.length, deadline);
    if (error) {
        return TcpStreamResult{nullptr, error};
    }
    return result;
}

TcpStreamResult TcpStream::connect(Poller &poller, std::vector<SocketAddress> const &addresses,
                                   Deadline const deadline) {
    TcpStreamResult result{nullptr, std::make_error_code(std::errc::address_not_available)};
    for (SocketAddress const &address : addresses) {
        result = connect(poller, address, deadline);
        if (result or result.error == std::errc::timed_out) {
            break;
        }
    }
    return result;
}

TcpStreamResult TcpStream::adopt(Poller &poller, Fd socket) {
    if (std::error_code const error = setNonBlocking(socket.get())) {
        return TcpStreamResult{nullptr, error};
    }
    return wrap(poller, std::move(socket));
}

std::error_code TcpStream::setNoDelay(bool const enabled) noexcept {
    return enableNoDelay(fd(), enabled);
}

// --- TcpListener -----------------------------------------------------------------

TcpListenerResult TcpListener::bind(Poller &poller, SocketAddress const &address, int const backlog) {
    TcpListenOptions options;
    options.backlog = backlog;
    return bind(poller, address, options);
}

TcpListenerResult TcpListener::bind(Poller &poller, SocketAddress const &address, TcpListenOptions const &options) {
    Fd socket;
    if (std::error_code const error = detail::openSocket(address.family(), SOCK_STREAM, socket)) {
        return TcpListenerResult{nullptr, error};
    }
    int const one = 1;
    ::setsockopt(socket.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (options.reusePort) {
#if defined(SO_REUSEPORT)
        if (::setsockopt(socket.get(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof one) != 0) {
            return TcpListenerResult{nullptr, lastError()};
        }
#else
        return TcpListenerResult{nullptr, std::make_error_code(std::errc::function_not_supported)};
#endif
    }
    if (std::error_code const error = detail::bindAndListen(socket.get(), address, options.backlog)) {
        return TcpListenerResult{nullptr, error};
    }
    SocketAddress const local = detail::localAddressOf(socket.get());
    std::unique_ptr<TcpListener> listener(new TcpListener(poller, std::move(socket), local));
    if (std::error_code const error = listener->registration.error()) {
        return TcpListenerResult{nullptr, error};
    }
    return TcpListenerResult{std::move(listener), std::error_code{}};
}

TcpStreamResult TcpListener::acceptUntil(Deadline const deadline) {
    AcceptResult accepted =
        deadline == Deadline::max() ? io::accept(registration) : io::acceptUntil(registration, deadline);
    if (not accepted) {
        return TcpStreamResult{nullptr, accepted.error};
    }
    return TcpStream::wrap(poller, std::move(accepted.fd)); // accept hands out non-blocking sockets
}

} // namespace io
} // namespace stackfull
