#pragma once

#include <cstdint>

namespace stackfull {
namespace sched {

namespace detail {
struct SchedulerCore;
}

// Handle for waking a parked task from any thread. Cheap to copy, safe to
// keep after the task finished: it addresses a slab slot plus a generation,
// so a stale token wakes nothing instead of touching freed memory.
//
// Semantics are token-like: a wake() delivered while the task is running is
// remembered and makes its next park() return at once. Spurious returns from
// park() are allowed, so callers re-check their condition in a loop.
struct WakeToken {
    // False if the task had already finished.
    bool wake() const noexcept;

    explicit operator bool() const noexcept { return core != nullptr; }

    detail::SchedulerCore *core = nullptr;
    std::uint32_t slot = 0;
    std::uint32_t generation = 0;
};

} // namespace sched
} // namespace stackfull
