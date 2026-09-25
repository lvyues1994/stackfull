#pragma once

// Socket plumbing shared by TcpStream, UnixStream and UdpSocket.

#include <stackfull/io/Fd.h>
#include <stackfull/io/SocketAddress.h>

#include <system_error>

#include <sys/socket.h>

namespace stackfull {
namespace io {
namespace detail {

#if defined(MSG_NOSIGNAL)
constexpr int kNoSigPipe = MSG_NOSIGNAL;
#else
constexpr int kNoSigPipe = 0;
#endif

// Non-blocking, close-on-exec socket of `family` and `type`.
std::error_code openSocket(int family, int type, Fd &out) noexcept;

// Where send() has no MSG_NOSIGNAL, tell the socket itself not to raise
// SIGPIPE if the platform can.
void suppressSigPipe(int fd) noexcept;

std::error_code bindAndListen(int fd, SocketAddress const &address, int backlog) noexcept;

SocketAddress localAddressOf(int fd) noexcept;
SocketAddress peerAddressOf(int fd) noexcept;

} // namespace detail
} // namespace io
} // namespace stackfull
