#pragma once

#include <cstdint>
#include <string>

#include <sys/socket.h>

namespace stackfull {
namespace io {

// An IPv4, IPv6 or Unix-domain endpoint.
struct SocketAddress {
    sockaddr_storage storage{};
    socklen_t length = 0; // 0: no address

    // Numeric host only ("127.0.0.1", "::1"); false if it does not parse.
    // Names go through io::resolve() or stackfull::resolve().
    static bool parse(char const *host, std::uint16_t port, SocketAddress &out) noexcept;
    static SocketAddress loopback(std::uint16_t port) noexcept;   // 127.0.0.1
    static SocketAddress loopbackV6(std::uint16_t port) noexcept; // ::1
    static SocketAddress any(std::uint16_t port) noexcept;        // 0.0.0.0
    static SocketAddress anyV6(std::uint16_t port) noexcept;      // ::
    // A filesystem path (false if longer than sockaddr_un allows).
    static bool unixPath(char const *path, SocketAddress &out) noexcept;
#if defined(__linux__)
    // Linux/Android abstract namespace: no file, gone with the last socket.
    static bool unixAbstract(char const *name, SocketAddress &out) noexcept;
#endif

    sockaddr const *get() const noexcept { return reinterpret_cast<sockaddr const *>(&storage); }
    int family() const noexcept { return storage.ss_family; }
    std::uint16_t port() const noexcept; // 0 for Unix-domain addresses
    // "127.0.0.1:80", "[::1]:80", "unix:/run/app.sock", "unix:@name".
    std::string toString() const;
};

} // namespace io
} // namespace stackfull
