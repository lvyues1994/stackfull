#pragma once

#include <stackfull/io/SocketAddress.h>

#include <cstdint>
#include <system_error>
#include <vector>

#include <sys/socket.h>

namespace stackfull {
namespace io {

struct ResolveResult {
    std::vector<SocketAddress> addresses; // in the resolver's order of preference
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// getaddrinfo() error codes (EAI_*), with gai_strerror() messages.
std::error_category const &resolveCategory() noexcept;

// Addresses for TCP (or UDP with `socketType` = SOCK_DGRAM) to `host`:
// numeric hosts parse at once, names go through getaddrinfo(). That call
// blocks the thread for as long as the lookup takes — from a task, use
// stackfull::resolve() (runtime), which runs it on the blocking pool.
// `family`: AF_UNSPEC (both), AF_INET or AF_INET6.
ResolveResult resolve(char const *host, std::uint16_t port, int family = AF_UNSPEC, int socketType = SOCK_STREAM);

} // namespace io
} // namespace stackfull
