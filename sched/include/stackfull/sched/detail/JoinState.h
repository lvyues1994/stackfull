#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/sched/Parker.h>
#include <stackfull/sched/WakeToken.h>

#include <atomic>
#include <cstdint>
#include <memory>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

namespace stackfull {
namespace sched {
namespace detail {

// Shared between a joinable task and its JoinHandle; heap allocated,
// reference counted (task + handle). At most one joiner at a time.
struct JoinState {
    enum : std::uint8_t { kNoWaiter = 0, kTaskWaiter = 1, kThreadWaiter = 2 };

    std::atomic<int> refs{2};
    std::atomic<bool> done{false};
    std::atomic<std::uint8_t> waiterKind{kNoWaiter};
    WakeToken taskWaiter{};
    // Created by a joining OS thread; owned here so the finishing task may
    // still unpark it after the joiner returned.
    std::unique_ptr<Parker> threadWaiter;
#if STACKFULL_HAS_EXCEPTIONS
    std::exception_ptr exception;
#endif

    void release() noexcept {
        if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }
};

} // namespace detail
} // namespace sched
} // namespace stackfull
