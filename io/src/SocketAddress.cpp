#include <stackfull/io/SocketAddress.h>

#include <cstddef>
#include <cstdio>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/un.h>

namespace stackfull {
namespace io {

namespace {

SocketAddress ipv4(std::uint32_t const hostOrder, std::uint16_t const port) noexcept {
    SocketAddress result;
    auto *const v4 = reinterpret_cast<sockaddr_in *>(&result.storage);
    v4->sin_family = AF_INET;
    v4->sin_port = htons(port);
    v4->sin_addr.s_addr = htonl(hostOrder);
    result.length = sizeof(sockaddr_in);
    return result;
}

SocketAddress ipv6(in6_addr const &address, std::uint16_t const port) noexcept {
    SocketAddress result;
    auto *const v6 = reinterpret_cast<sockaddr_in6 *>(&result.storage);
    v6->sin6_family = AF_INET6;
    v6->sin6_port = htons(port);
    v6->sin6_addr = address;
    result.length = sizeof(sockaddr_in6);
    return result;
}

// `prefix` bytes (0 or the abstract namespace's leading NUL) then `name`.
bool unixAddress(char const *const name, std::size_t const prefix, bool const terminate, SocketAddress &out) noexcept {
    SocketAddress result;
    auto *const un = reinterpret_cast<sockaddr_un *>(&result.storage);
    std::size_t const length = std::strlen(name);
    if (prefix + length + (terminate ? 1 : 0) > sizeof un->sun_path) {
        return false;
    }
    un->sun_family = AF_UNIX;
    std::memcpy(un->sun_path + prefix, name, length);
    result.length = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + prefix + length + (terminate ? 1 : 0));
    out = result;
    return true;
}

} // namespace

bool SocketAddress::parse(char const *const host, std::uint16_t const port, SocketAddress &out) noexcept {
    in_addr v4{};
    if (::inet_pton(AF_INET, host, &v4) == 1) {
        out = ipv4(ntohl(v4.s_addr), port);
        return true;
    }
    in6_addr v6{};
    if (::inet_pton(AF_INET6, host, &v6) == 1) {
        out = ipv6(v6, port);
        return true;
    }
    return false;
}

SocketAddress SocketAddress::loopback(std::uint16_t const port) noexcept {
    return ipv4(INADDR_LOOPBACK, port);
}

SocketAddress SocketAddress::loopbackV6(std::uint16_t const port) noexcept {
    return ipv6(in6addr_loopback, port);
}

SocketAddress SocketAddress::any(std::uint16_t const port) noexcept {
    return ipv4(INADDR_ANY, port);
}

SocketAddress SocketAddress::anyV6(std::uint16_t const port) noexcept {
    return ipv6(in6addr_any, port);
}

bool SocketAddress::unixPath(char const *const path, SocketAddress &out) noexcept {
    return unixAddress(path, 0, true, out);
}

#if defined(__linux__)
bool SocketAddress::unixAbstract(char const *const name, SocketAddress &out) noexcept {
    return unixAddress(name, 1, false, out);
}
#endif

std::uint16_t SocketAddress::port() const noexcept {
    if (storage.ss_family == AF_INET) {
        return ntohs(reinterpret_cast<sockaddr_in const *>(&storage)->sin_port);
    }
    if (storage.ss_family == AF_INET6) {
        return ntohs(reinterpret_cast<sockaddr_in6 const *>(&storage)->sin6_port);
    }
    return 0;
}

std::string SocketAddress::toString() const {
    char text[INET6_ADDRSTRLEN + 16] = {};
    if (storage.ss_family == AF_INET) {
        auto const *const v4 = reinterpret_cast<sockaddr_in const *>(&storage);
        char host[INET_ADDRSTRLEN] = {};
        ::inet_ntop(AF_INET, &v4->sin_addr, host, sizeof host);
        std::snprintf(text, sizeof text, "%s:%u", host, static_cast<unsigned>(port()));
        return text;
    }
    if (storage.ss_family == AF_INET6) {
        auto const *const v6 = reinterpret_cast<sockaddr_in6 const *>(&storage);
        char host[INET6_ADDRSTRLEN] = {};
        ::inet_ntop(AF_INET6, &v6->sin6_addr, host, sizeof host);
        std::snprintf(text, sizeof text, "[%s]:%u", host, static_cast<unsigned>(port()));
        return text;
    }
    if (storage.ss_family == AF_UNIX) {
        auto const *const un = reinterpret_cast<sockaddr_un const *>(&storage);
        auto const total = static_cast<std::size_t>(length); // socklen_t is signed on some ABIs
        std::size_t const pathBytes =
            total > offsetof(sockaddr_un, sun_path) ? total - offsetof(sockaddr_un, sun_path) : 0;
        if (pathBytes != 0 and un->sun_path[0] == '\0') {
            return "unix:@" + std::string(un->sun_path + 1, pathBytes - 1);
        }
        return "unix:" + std::string(un->sun_path, ::strnlen(un->sun_path, pathBytes));
    }
    return "(none)";
}

} // namespace io
} // namespace stackfull
