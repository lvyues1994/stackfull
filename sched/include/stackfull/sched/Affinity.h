#pragma once

#include <cstddef>
#include <initializer_list>
#include <system_error>

namespace stackfull {
namespace sched {

// Restricts the calling thread to the given CPUs (Linux/Android:
// sched_setaffinity; QNX: the thread's runmask, CPUs 0..31). Typically
// called from SchedulerOptions::onWorkerStart, e.g. to keep workers on the
// big cores of a big.LITTLE SoC. std::errc::function_not_supported elsewhere.
std::error_code pinCurrentThreadToCpus(int const *cpus, std::size_t count) noexcept;

inline std::error_code pinCurrentThreadToCpus(std::initializer_list<int> const cpus) noexcept {
    return pinCurrentThreadToCpus(cpus.begin(), cpus.size());
}

} // namespace sched
} // namespace stackfull
