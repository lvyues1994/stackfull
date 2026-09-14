#pragma once

#include <stackfull/io/Fd.h>
#include <stackfull/io/Registration.h>

#include <cstddef>
#include <system_error>

#include <sys/socket.h>

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

// Blocking-style wrappers over non-blocking descriptors: they retry on
// EINTR and park the caller (task or thread) on EAGAIN until the Poller
// reports readiness. The descriptor must be O_NONBLOCK and registered.

IoResult read(Registration &registration, void *buffer, std::size_t length);
IoResult write(Registration &registration, void const *buffer, std::size_t length);

// Loops write() until everything is sent or an error occurs.
IoResult writeAll(Registration &registration, void const *buffer, std::size_t length);

// Loops read() until `length` bytes arrived; bytes < length means EOF.
IoResult readExactly(Registration &registration, void *buffer, std::size_t length);

AcceptResult accept(Registration &listener);

// Non-blocking connect: starts it, waits for writability, reports SO_ERROR.
std::error_code connect(Registration &registration, sockaddr const *address, socklen_t length);

} // namespace io
} // namespace stackfull
