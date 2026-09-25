#pragma once

#include <stackfull/io/Fd.h>
#include <stackfull/io/Registration.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>

#include <sys/socket.h>
#include <sys/uio.h>

namespace stackfull {
namespace io {

// Result of one transfer. `bytes == 0` with no error means end of stream
// for read(); write() returns whatever the kernel accepted (may be short).
struct IoResult {
    std::size_t bytes = 0;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

struct AcceptResult {
    Fd fd; // non-blocking already
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

using Deadline = std::chrono::steady_clock::time_point;

// Blocking-style wrappers over non-blocking descriptors: they retry on
// EINTR and park the caller (task or thread) on EAGAIN until the Poller
// reports readiness. The descriptor must be O_NONBLOCK and registered.
// The ...Until variants give up at `deadline` with std::errc::timed_out
// (for writeAllUntil, after whatever was sent so far).
//
// Registration::setKind() picks the system calls: sockets use recv/send
// (writes to a reset connection fail with EPIPE rather than raise SIGPIPE),
// everything else read/write — where a write to a pipe without readers
// still raises SIGPIPE unless the process ignores it.

IoResult read(Registration &registration, void *buffer, std::size_t length);
IoResult readUntil(Registration &registration, void *buffer, std::size_t length, Deadline deadline);
IoResult write(Registration &registration, void const *buffer, std::size_t length);

// Loops write() until everything is sent or an error occurs.
IoResult writeAll(Registration &registration, void const *buffer, std::size_t length);
IoResult writeAllUntil(Registration &registration, void const *buffer, std::size_t length, Deadline deadline);

// Loops read() until `length` bytes arrived; bytes < length means EOF.
IoResult readExactly(Registration &registration, void *buffer, std::size_t length);

// Scatter/gather. writevAll() loops until every vector is sent, advancing
// `vectors` in place.
IoResult readv(Registration &registration, iovec const *vectors, int count);
IoResult writev(Registration &registration, iovec const *vectors, int count);
IoResult writevAll(Registration &registration, iovec *vectors, int count);

AcceptResult accept(Registration &listener);
AcceptResult acceptUntil(Registration &listener, Deadline deadline);

namespace detail {

inline std::error_code waitNewer(Registration &registration, Interest const direction, std::uint32_t const seen,
                                 Deadline const *const deadline) {
    if (direction == Interest::Readable) {
        return deadline != nullptr ? registration.waitReadableUntil(seen, *deadline) : registration.waitReadable(seen);
    }
    return deadline != nullptr ? registration.waitWritableUntil(seen, *deadline) : registration.waitWritable(seen);
}

} // namespace detail

// The loop behind every transfer above, for other non-blocking system calls
// on a registered descriptor (recvfrom, recvmmsg, ...). Calls `call` — which
// returns ssize_t, or -1 with errno — until it succeeds, fails with
// something other than EINTR/EAGAIN, or `deadline` (null: none) passes while
// waiting for `direction`. The event count is read before each call, so a
// report landing between the call and the wait is not lost. `asked` is the
// byte count requested: on a byte stream a shorter success lets the next
// call wait without asking the kernel (Registration::setKind).
template <class Call>
IoResult callWhenReady(Registration &registration, Interest const direction, std::size_t const asked,
                       Deadline const *const deadline, Call const &call) {
    for (;;) {
        std::uint32_t const seen =
            direction == Interest::Readable ? registration.readEvents() : registration.writeEvents();
        if (registration.exhausted(direction, seen)) {
            if (std::error_code const error = detail::waitNewer(registration, direction, seen, deadline)) {
                return IoResult{0, error};
            }
            continue;
        }
        auto const n = call();
        if (n >= 0) {
            registration.noteTransfer(direction, seen, static_cast<std::size_t>(n), asked);
            return IoResult{static_cast<std::size_t>(n), std::error_code{}};
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN and errno != EWOULDBLOCK) {
            return IoResult{0, lastError()};
        }
        if (std::error_code const error = detail::waitNewer(registration, direction, seen, deadline)) {
            return IoResult{0, error};
        }
    }
}

// Non-blocking connect: starts it, waits for writability, reports SO_ERROR.
std::error_code connect(Registration &registration, sockaddr const *address, socklen_t length);
std::error_code connectUntil(Registration &registration, sockaddr const *address, socklen_t length, Deadline deadline);

} // namespace io
} // namespace stackfull
