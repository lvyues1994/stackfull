#include <stackfull/io/Tcp.h>

#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace stackfull {
namespace io {

sockaddr_in loopbackAddress(std::uint16_t const port) noexcept {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return address;
}

TcpSocketResult makeTcpSocket() noexcept {
    Fd fd{::socket(AF_INET, SOCK_STREAM, 0)};
    if (not fd) {
        return TcpSocketResult{Fd{}, lastError()};
    }
    if (std::error_code const error = setNonBlocking(fd.get())) {
        return TcpSocketResult{Fd{}, error};
    }
    int const one = 1;
    ::setsockopt(fd.get(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return TcpSocketResult{std::move(fd), std::error_code{}};
}

TcpListenResult listenTcpLoopback(std::uint16_t const port, int const backlog) noexcept {
    TcpSocketResult socket = makeTcpSocket();
    if (not socket) {
        return TcpListenResult{Fd{}, 0, socket.error};
    }
    int const one = 1;
    ::setsockopt(socket.fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    sockaddr_in address = loopbackAddress(port);
    if (::bind(socket.fd.get(), reinterpret_cast<sockaddr const *>(&address), sizeof address) != 0) {
        return TcpListenResult{Fd{}, 0, lastError()};
    }
    if (::listen(socket.fd.get(), backlog) != 0) {
        return TcpListenResult{Fd{}, 0, lastError()};
    }
    socklen_t length = sizeof address;
    if (::getsockname(socket.fd.get(), reinterpret_cast<sockaddr *>(&address), &length) != 0) {
        return TcpListenResult{Fd{}, 0, lastError()};
    }
    return TcpListenResult{std::move(socket.fd), ntohs(address.sin_port), std::error_code{}};
}

} // namespace io
} // namespace stackfull
