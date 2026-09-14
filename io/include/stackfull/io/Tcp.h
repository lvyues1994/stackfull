#pragma once

#include <stackfull/io/Fd.h>

#include <cstdint>
#include <system_error>

#include <netinet/in.h>

namespace stackfull {
namespace io {

struct TcpListenResult {
    Fd fd; // non-blocking, listening
    std::uint16_t port = 0;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// IPv4 loopback listener; port 0 picks a free one and reports it.
TcpListenResult listenTcpLoopback(std::uint16_t port = 0, int backlog = 128) noexcept;

struct TcpSocketResult {
    Fd fd; // non-blocking, unconnected
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

TcpSocketResult makeTcpSocket() noexcept;

sockaddr_in loopbackAddress(std::uint16_t port) noexcept;

} // namespace io
} // namespace stackfull
