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

// One attempt that never waits, for operations that wait their own way
// (stackfull::exec): read the direction's event count as `seen` first; on
// `wouldBlock`, wait for a report newer than `seen` and try again.
struct IoAttempt {
    std::size_t bytes = 0;
    std::error_code error;
    bool wouldBlock = false;
};

inline std::uint32_t eventsOf(Registration const &registration, Interest const direction) noexcept {
    return direction == Interest::Readable ? registration.readEvents() : registration.writeEvents();
}

// One non-blocking system call on a registered descriptor: `call` returns
// ssize_t, or -1 with errno; EINTR is retried. `asked` is the byte count
// requested: on a byte stream a shorter success makes the next attempt wait
// without asking the kernel (Registration::setKind) — `wouldBlock` at once.
template <class Call>
IoAttempt tryCall(Registration &registration, Interest const direction, std::size_t const asked,
                  std::uint32_t const seen, Call const &call) {
    if (registration.exhausted(direction, seen)) {
        return IoAttempt{0, std::error_code{}, true};
    }
    for (;;) {
        auto const n = call();
        if (n >= 0) {
            registration.noteTransfer(direction, seen, static_cast<std::size_t>(n), asked);
            return IoAttempt{static_cast<std::size_t>(n), std::error_code{}, false};
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN or errno == EWOULDBLOCK) {
            return IoAttempt{0, std::error_code{}, true};
        }
        return IoAttempt{0, lastError(), false};
    }
}

// read() / write() / accept() as one attempt each.
IoAttempt tryRead(Registration &registration, std::uint32_t seen, void *buffer, std::size_t length);
IoAttempt tryWrite(Registration &registration, std::uint32_t seen, void const *buffer, std::size_t length);
AcceptResult tryAccept(Registration &listener, bool &wouldBlock);

// Repeats `attempt(seen)` — returning an IoAttempt — until it does not
// block, waiting for `direction` in between, or `deadline` (null: none)
// passes while waiting. The event count is read before each attempt, so a
// report landing between the attempt and the wait is not lost.
template <class Attempt>
IoResult retryWhenReady(Registration &registration, Interest const direction, Deadline const *const deadline,
                        Attempt const &attempt) {
    for (;;) {
        std::uint32_t const seen = eventsOf(registration, direction);
        IoAttempt const result = attempt(seen);
        if (not result.wouldBlock) {
            return IoResult{result.bytes, result.error};
        }
        if (std::error_code const error = detail::waitNewer(registration, direction, seen, deadline)) {
            return IoResult{0, error};
        }
    }
}

// The loop behind every transfer above, for other non-blocking system calls
// on a registered descriptor (recvfrom, recvmmsg, ...): tryCall() until it
// does not hit EAGAIN.
template <class Call>
IoResult callWhenReady(Registration &registration, Interest const direction, std::size_t const asked,
                       Deadline const *const deadline, Call const &call) {
    return retryWhenReady(registration, direction, deadline, [&](std::uint32_t const seen) {
        return tryCall(registration, direction, asked, seen, call);
    });
}

// Non-blocking connect: starts it, waits for writability, reports SO_ERROR.
// Registering an unconnected TCP socket reports it writable (and hung up)
// at once, which these may take for the handshake's end: prefer starting
// the connect before registering, as below.
std::error_code connect(Registration &registration, sockaddr const *address, socklen_t length);
std::error_code connectUntil(Registration &registration, sockaddr const *address, socklen_t length, Deadline deadline);

// Connecting in two steps, around registering the socket: startConnect()
// on the bare descriptor (no error and not `inProgress`: connected), then
// register it, wait for its first write report, and finishConnect() for the
// outcome.
std::error_code startConnect(int fd, sockaddr const *address, socklen_t length, bool &inProgress) noexcept;
std::error_code finishConnect(int fd) noexcept;

} // namespace io
} // namespace stackfull
