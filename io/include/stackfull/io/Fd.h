#pragma once

#include <system_error>

namespace stackfull {
namespace io {

// Owning file descriptor. Move-only; closes on destruction.
struct Fd {
    Fd() noexcept = default;
    explicit Fd(int const fd_) noexcept : fd(fd_) {}
    Fd(Fd const &) = delete;
    Fd &operator=(Fd const &) = delete;
    Fd(Fd &&other) noexcept : fd(other.fd) { other.fd = -1; }
    Fd &operator=(Fd &&other) noexcept;
    ~Fd() { reset(); }

    explicit operator bool() const noexcept { return fd >= 0; }
    int get() const noexcept { return fd; }

    // Gives up ownership without closing.
    int release() noexcept {
        int const value = fd;
        fd = -1;
        return value;
    }

    void reset() noexcept;

private:
    int fd = -1;
};

// O_NONBLOCK on; every descriptor used with the async wrappers must have it.
std::error_code setNonBlocking(int fd) noexcept;

// errno as a std::error_code.
std::error_code lastError() noexcept;

} // namespace io
} // namespace stackfull
