#include <stackfull/io/Fd.h>

#include <cerrno>
#include <system_error>

#include <fcntl.h>
#include <unistd.h>

namespace stackfull {
namespace io {

Fd &Fd::operator=(Fd &&other) noexcept {
    if (this != &other) {
        reset();
        fd = other.fd;
        other.fd = -1;
    }
    return *this;
}

void Fd::reset() noexcept {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

std::error_code lastError() noexcept {
    return std::error_code(errno, std::generic_category());
}

std::error_code setNonBlocking(int const fd) noexcept {
    int const flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return lastError();
    }
    if ((flags & O_NONBLOCK) == 0 and ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return lastError();
    }
    return std::error_code{};
}

} // namespace io
} // namespace stackfull
