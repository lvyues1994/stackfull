#include <stackfull/io/Async.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include <sys/socket.h>
#include <unistd.h>

namespace stackfull {
namespace io {

namespace {

bool wouldBlock(int const error) noexcept {
    return error == EAGAIN or error == EWOULDBLOCK;
}

} // namespace

// Every operation snapshots the direction's event count *before* its system
// call and, on EAGAIN, waits for a newer report: with edge triggering a
// report that arrives between the two is not lost (see Registration).

IoResult read(Registration &registration, void *const buffer, std::size_t const length) {
    for (;;) {
        std::uint32_t const seen = registration.readEvents();
        ssize_t const n = ::read(registration.fd(), buffer, length);
        if (n >= 0) {
            return IoResult{static_cast<std::size_t>(n), std::error_code{}};
        }
        if (errno == EINTR) {
            continue;
        }
        if (not wouldBlock(errno)) {
            return IoResult{0, lastError()};
        }
        if (std::error_code const error = registration.waitReadable(seen)) {
            return IoResult{0, error};
        }
    }
}

IoResult write(Registration &registration, void const *const buffer, std::size_t const length) {
    for (;;) {
        std::uint32_t const seen = registration.writeEvents();
        ssize_t const n = ::write(registration.fd(), buffer, length);
        if (n >= 0) {
            return IoResult{static_cast<std::size_t>(n), std::error_code{}};
        }
        if (errno == EINTR) {
            continue;
        }
        if (not wouldBlock(errno)) {
            return IoResult{0, lastError()};
        }
        if (std::error_code const error = registration.waitWritable(seen)) {
            return IoResult{0, error};
        }
    }
}

IoResult writeAll(Registration &registration, void const *const buffer, std::size_t const length) {
    std::size_t sent = 0;
    while (sent < length) {
        IoResult const chunk = write(registration, static_cast<char const *>(buffer) + sent, length - sent);
        if (not chunk) {
            return IoResult{sent, chunk.error};
        }
        sent += chunk.bytes;
    }
    return IoResult{sent, std::error_code{}};
}

IoResult readExactly(Registration &registration, void *const buffer, std::size_t const length) {
    std::size_t received = 0;
    while (received < length) {
        IoResult const chunk = read(registration, static_cast<char *>(buffer) + received, length - received);
        if (not chunk) {
            return IoResult{received, chunk.error};
        }
        if (chunk.bytes == 0) {
            break; // EOF
        }
        received += chunk.bytes;
    }
    return IoResult{received, std::error_code{}};
}

AcceptResult accept(Registration &listener) {
    for (;;) {
        std::uint32_t const seen = listener.readEvents();
#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
        int const fd = ::accept4(listener.fd(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
        int const fd = ::accept(listener.fd(), nullptr, nullptr);
#endif
        if (fd >= 0) {
            Fd accepted{fd};
#if !(defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC))
            if (std::error_code const error = setNonBlocking(fd)) {
                return AcceptResult{Fd{}, error};
            }
#endif
            return AcceptResult{std::move(accepted), std::error_code{}};
        }
        if (errno == EINTR) {
            continue;
        }
        if (not wouldBlock(errno)) {
            return AcceptResult{Fd{}, lastError()};
        }
        if (std::error_code const error = listener.waitReadable(seen)) {
            return AcceptResult{Fd{}, error};
        }
    }
}

std::error_code connect(Registration &registration, sockaddr const *const address, socklen_t const length) {
    std::uint32_t const seen = registration.writeEvents();
    for (;;) {
        if (::connect(registration.fd(), address, length) == 0) {
            return std::error_code{};
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EISCONN) {
            return std::error_code{};
        }
        if (errno != EINPROGRESS and errno != EALREADY and not wouldBlock(errno)) {
            return lastError();
        }
        break;
    }
    if (std::error_code const error = registration.waitWritable(seen)) {
        return error;
    }
    int soError = 0;
    socklen_t soLength = sizeof soError;
    if (::getsockopt(registration.fd(), SOL_SOCKET, SO_ERROR, &soError, &soLength) != 0) {
        return lastError();
    }
    return soError == 0 ? std::error_code{} : std::error_code(soError, std::generic_category());
}

} // namespace io
} // namespace stackfull
