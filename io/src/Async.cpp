#include <stackfull/io/Async.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

namespace stackfull {
namespace io {

namespace {

bool wouldBlock(int const error) noexcept {
    return error == EAGAIN or error == EWOULDBLOCK;
}

// Every operation snapshots the direction's event count *before* its system
// call and, on EAGAIN, waits for a newer report: with edge triggering a
// report that arrives between the two is not lost (see Registration).
// `deadline` null means no timeout.
std::error_code waitAgain(Registration &registration, Interest const direction, std::uint32_t const seen,
                          Deadline const *const deadline) {
    if (direction == Interest::Readable) {
        return deadline != nullptr ? registration.waitReadableUntil(seen, *deadline) : registration.waitReadable(seen);
    }
    return deadline != nullptr ? registration.waitWritableUntil(seen, *deadline) : registration.waitWritable(seen);
}

std::uint32_t eventsOf(Registration const &registration, Interest const direction) noexcept {
    return direction == Interest::Readable ? registration.readEvents() : registration.writeEvents();
}

// Retries `transfer` (a read/write-like call returning ssize_t) until it
// moves bytes, fails for real, or the deadline passes.
template <class Transfer>
IoResult transferOnce(Registration &registration, Interest const direction, Deadline const *const deadline,
                      Transfer const &transfer) {
    for (;;) {
        std::uint32_t const seen = eventsOf(registration, direction);
        ssize_t const n = transfer();
        if (n >= 0) {
            return IoResult{static_cast<std::size_t>(n), std::error_code{}};
        }
        if (errno == EINTR) {
            continue;
        }
        if (not wouldBlock(errno)) {
            return IoResult{0, lastError()};
        }
        if (std::error_code const error = waitAgain(registration, direction, seen, deadline)) {
            return IoResult{0, error};
        }
    }
}

IoResult readImpl(Registration &registration, void *const buffer, std::size_t const length,
                  Deadline const *const deadline) {
    return transferOnce(registration, Interest::Readable, deadline,
                        [&] { return ::read(registration.fd(), buffer, length); });
}

IoResult writeImpl(Registration &registration, void const *const buffer, std::size_t const length,
                   Deadline const *const deadline) {
    return transferOnce(registration, Interest::Writable, deadline,
                        [&] { return ::write(registration.fd(), buffer, length); });
}

IoResult writeAllImpl(Registration &registration, void const *const buffer, std::size_t const length,
                      Deadline const *const deadline) {
    std::size_t sent = 0;
    while (sent < length) {
        IoResult const chunk = writeImpl(registration, static_cast<char const *>(buffer) + sent, length - sent, deadline);
        if (not chunk) {
            return IoResult{sent, chunk.error};
        }
        sent += chunk.bytes;
    }
    return IoResult{sent, std::error_code{}};
}

AcceptResult acceptImpl(Registration &listener, Deadline const *const deadline) {
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
        if (std::error_code const error = waitAgain(listener, Interest::Readable, seen, deadline)) {
            return AcceptResult{Fd{}, error};
        }
    }
}

std::error_code connectImpl(Registration &registration, sockaddr const *const address, socklen_t const length,
                            Deadline const *const deadline) {
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
    if (std::error_code const error = waitAgain(registration, Interest::Writable, seen, deadline)) {
        return error;
    }
    int soError = 0;
    socklen_t soLength = sizeof soError;
    if (::getsockopt(registration.fd(), SOL_SOCKET, SO_ERROR, &soError, &soLength) != 0) {
        return lastError();
    }
    return soError == 0 ? std::error_code{} : std::error_code(soError, std::generic_category());
}

} // namespace

IoResult read(Registration &registration, void *const buffer, std::size_t const length) {
    return readImpl(registration, buffer, length, nullptr);
}

IoResult readUntil(Registration &registration, void *const buffer, std::size_t const length, Deadline const deadline) {
    return readImpl(registration, buffer, length, &deadline);
}

IoResult write(Registration &registration, void const *const buffer, std::size_t const length) {
    return writeImpl(registration, buffer, length, nullptr);
}

IoResult writeAll(Registration &registration, void const *const buffer, std::size_t const length) {
    return writeAllImpl(registration, buffer, length, nullptr);
}

IoResult writeAllUntil(Registration &registration, void const *const buffer, std::size_t const length,
                       Deadline const deadline) {
    return writeAllImpl(registration, buffer, length, &deadline);
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

IoResult readv(Registration &registration, iovec const *const vectors, int const count) {
    return transferOnce(registration, Interest::Readable, nullptr,
                        [&] { return ::readv(registration.fd(), vectors, count); });
}

IoResult writev(Registration &registration, iovec const *const vectors, int const count) {
    return transferOnce(registration, Interest::Writable, nullptr,
                        [&] { return ::writev(registration.fd(), vectors, count); });
}

IoResult writevAll(Registration &registration, iovec *vectors, int count) {
    std::size_t sent = 0;
    while (count > 0) {
        IoResult const chunk = writev(registration, vectors, count);
        if (not chunk) {
            return IoResult{sent, chunk.error};
        }
        sent += chunk.bytes;
        std::size_t left = chunk.bytes;
        while (count > 0 and left >= vectors->iov_len) {
            left -= vectors->iov_len;
            ++vectors;
            --count;
        }
        if (count > 0) {
            vectors->iov_base = static_cast<char *>(vectors->iov_base) + left;
            vectors->iov_len -= left;
        }
    }
    return IoResult{sent, std::error_code{}};
}

AcceptResult accept(Registration &listener) {
    return acceptImpl(listener, nullptr);
}

AcceptResult acceptUntil(Registration &listener, Deadline const deadline) {
    return acceptImpl(listener, &deadline);
}

std::error_code connect(Registration &registration, sockaddr const *const address, socklen_t const length) {
    return connectImpl(registration, address, length, nullptr);
}

std::error_code connectUntil(Registration &registration, sockaddr const *const address, socklen_t const length,
                             Deadline const deadline) {
    return connectImpl(registration, address, length, &deadline);
}

} // namespace io
} // namespace stackfull
