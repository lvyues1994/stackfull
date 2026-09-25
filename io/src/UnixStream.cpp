#include <stackfull/io/UnixStream.h>

#include "Socket.h"

#include <utility>

#include <sys/socket.h>

namespace stackfull {
namespace io {

// --- UnixStream ------------------------------------------------------------------

UnixStreamResult UnixStream::wrap(Poller &poller, Fd socket) {
    detail::suppressSigPipe(socket.get());
    std::unique_ptr<UnixStream> stream(new UnixStream(poller, std::move(socket)));
    if (std::error_code const error = stream->events().error()) {
        return UnixStreamResult{nullptr, error};
    }
    return UnixStreamResult{std::move(stream), std::error_code{}};
}

UnixStreamResult UnixStream::connect(Poller &poller, SocketAddress const &address, Deadline const deadline) {
    if (address.family() != AF_UNIX) {
        return UnixStreamResult{nullptr, std::make_error_code(std::errc::address_family_not_supported)};
    }
    Fd socket;
    if (std::error_code const error = detail::openSocket(AF_UNIX, SOCK_STREAM, socket)) {
        return UnixStreamResult{nullptr, error};
    }
    UnixStreamResult result = wrap(poller, std::move(socket));
    if (not result) {
        return result;
    }
    Registration &registration = result.stream->events();
    std::error_code const error = deadline == Deadline::max()
                                      ? io::connect(registration, address.get(), address.length)
                                      : io::connectUntil(registration, address.get(), address.length, deadline);
    if (error) {
        return UnixStreamResult{nullptr, error};
    }
    return result;
}

UnixStreamPair UnixStream::pair(Poller &poller) {
    int ends[2] = {-1, -1};
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, ends) != 0) {
        return UnixStreamPair{nullptr, nullptr, lastError()};
    }
    Fd first{ends[0]};
    Fd second{ends[1]};
#else
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, ends) != 0) {
        return UnixStreamPair{nullptr, nullptr, lastError()};
    }
    Fd first{ends[0]};
    Fd second{ends[1]};
    for (int const fd : ends) {
        if (std::error_code const error = setNonBlocking(fd)) {
            return UnixStreamPair{nullptr, nullptr, error};
        }
    }
#endif
    UnixStreamResult a = wrap(poller, std::move(first));
    if (not a) {
        return UnixStreamPair{nullptr, nullptr, a.error};
    }
    UnixStreamResult b = wrap(poller, std::move(second));
    if (not b) {
        return UnixStreamPair{nullptr, nullptr, b.error};
    }
    return UnixStreamPair{std::move(a.stream), std::move(b.stream), std::error_code{}};
}

UnixStreamResult UnixStream::adopt(Poller &poller, Fd socket) {
    if (std::error_code const error = setNonBlocking(socket.get())) {
        return UnixStreamResult{nullptr, error};
    }
    return wrap(poller, std::move(socket));
}

// --- UnixListener ----------------------------------------------------------------

UnixListenerResult UnixListener::bind(Poller &poller, SocketAddress const &address, int const backlog) {
    if (address.family() != AF_UNIX) {
        return UnixListenerResult{nullptr, std::make_error_code(std::errc::address_family_not_supported)};
    }
    Fd socket;
    if (std::error_code const error = detail::openSocket(AF_UNIX, SOCK_STREAM, socket)) {
        return UnixListenerResult{nullptr, error};
    }
    if (std::error_code const error = detail::bindAndListen(socket.get(), address, backlog)) {
        return UnixListenerResult{nullptr, error};
    }
    std::unique_ptr<UnixListener> listener(new UnixListener(poller, std::move(socket), address));
    if (std::error_code const error = listener->registration.error()) {
        return UnixListenerResult{nullptr, error};
    }
    return UnixListenerResult{std::move(listener), std::error_code{}};
}

UnixStreamResult UnixListener::acceptUntil(Deadline const deadline) {
    AcceptResult accepted =
        deadline == Deadline::max() ? io::accept(registration) : io::acceptUntil(registration, deadline);
    if (not accepted) {
        return UnixStreamResult{nullptr, accepted.error};
    }
    return UnixStream::wrap(poller, std::move(accepted.fd));
}

} // namespace io
} // namespace stackfull
