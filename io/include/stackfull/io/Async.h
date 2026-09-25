#pragma once

#include <stackfull/io/Fd.h>
#include <stackfull/io/Registration.h>

#include <chrono>
#include <cstddef>
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

// Non-blocking connect: starts it, waits for writability, reports SO_ERROR.
std::error_code connect(Registration &registration, sockaddr const *address, socklen_t length);
std::error_code connectUntil(Registration &registration, sockaddr const *address, socklen_t length, Deadline deadline);

} // namespace io
} // namespace stackfull
