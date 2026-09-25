#include <stackfull/io/Resolve.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <memory>
#include <string>

#include <netdb.h>

namespace stackfull {
namespace io {

namespace {

struct ResolveCategory final : std::error_category {
    char const *name() const noexcept override { return "getaddrinfo"; }
    std::string message(int const code) const override { return ::gai_strerror(code); }
};

struct AddrInfoDeleter {
    void operator()(addrinfo *const list) const noexcept { ::freeaddrinfo(list); }
};

} // namespace

std::error_category const &resolveCategory() noexcept {
    static ResolveCategory const category;
    return category;
}

ResolveResult resolve(char const *const host, std::uint16_t const port, int const family, int const socketType) {
    ResolveResult result;
    SocketAddress numeric;
    if (SocketAddress::parse(host, port, numeric)) {
        if (family == AF_UNSPEC or family == numeric.family()) {
            result.addresses.push_back(numeric);
        } else {
            result.error = std::error_code(EAI_FAMILY, resolveCategory());
        }
        return result;
    }
    addrinfo hints{};
    hints.ai_family = family;
    hints.ai_socktype = socketType; // no AI_ADDRCONFIG: it drops "localhost" where only loopback is up
    std::string const service = std::to_string(port);
    addrinfo *list = nullptr;
    int const status = ::getaddrinfo(host, service.c_str(), &hints, &list);
    if (status != 0) {
#if defined(EAI_SYSTEM)
        if (status == EAI_SYSTEM) {
            result.error = std::error_code(errno, std::generic_category());
            return result;
        }
#endif
        result.error = std::error_code(status, resolveCategory());
        return result;
    }
    std::unique_ptr<addrinfo, AddrInfoDeleter> const owner(list);
    for (addrinfo const *entry = list; entry != nullptr; entry = entry->ai_next) {
        auto const bytes = static_cast<std::size_t>(entry->ai_addrlen); // socklen_t is signed on some ABIs
        if (bytes > sizeof(sockaddr_storage)) {
            continue;
        }
        SocketAddress address;
        std::memcpy(&address.storage, entry->ai_addr, bytes);
        address.length = static_cast<socklen_t>(entry->ai_addrlen);
        result.addresses.push_back(address);
    }
    if (result.addresses.empty()) {
        result.error = std::error_code(EAI_NONAME, resolveCategory());
    }
    return result;
}

} // namespace io
} // namespace stackfull
