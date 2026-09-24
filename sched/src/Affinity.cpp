#include <stackfull/sched/Affinity.h>

#include <cerrno>
#include <cstdint>

#if defined(__linux__)
#include <sched.h>
#elif defined(__QNX__)
#include <sys/neutrino.h>
#endif

namespace stackfull {
namespace sched {

std::error_code pinCurrentThreadToCpus(int const *const cpus, std::size_t const count) noexcept {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    for (std::size_t i = 0; i < count; ++i) {
        if (cpus[i] < 0 or cpus[i] >= CPU_SETSIZE) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        CPU_SET(static_cast<unsigned>(cpus[i]), &set);
    }
    if (::sched_setaffinity(0, sizeof set, &set) != 0) {
        return std::error_code(errno, std::generic_category());
    }
    return std::error_code{};
#elif defined(__QNX__)
    std::uint32_t mask = 0;
    for (std::size_t i = 0; i < count; ++i) {
        if (cpus[i] < 0 or cpus[i] >= 32) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        mask |= std::uint32_t{1} << cpus[i];
    }
    if (::ThreadCtl(_NTO_TCTL_RUNMASK, reinterpret_cast<void *>(static_cast<std::uintptr_t>(mask))) == -1) {
        return std::error_code(errno, std::generic_category());
    }
    return std::error_code{};
#else
    static_cast<void>(cpus);
    static_cast<void>(count);
    return std::make_error_code(std::errc::function_not_supported);
#endif
}

} // namespace sched
} // namespace stackfull
