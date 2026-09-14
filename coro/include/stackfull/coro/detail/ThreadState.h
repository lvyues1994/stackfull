#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/detail/ContextBlock.h>

namespace stackfull {
namespace coro {

// Everything the coroutine layer knows about one OS thread. Located through
// TLS exactly once per public API call (resume(), yield(), ...); the switch
// path itself never touches TLS — the departing context hands its ThreadState
// to the arriving one through the fcontext transfer.
struct ThreadState {
    // The thread's native stack, i.e. what runs when no coroutine does.
    detail::ContextBlock mainBlock{};
    // Context executing on this thread right now.
    detail::ContextBlock *current = nullptr;
    // Owned by the scheduler layer: the worker this thread is running, if
    // any. Opaque here so the coroutine layer stays independent of it.
    void *worker = nullptr;
};

namespace detail {

// Backend selected at build time (thread_local or pthread_key, see
// ThreadState.cpp). Never inlined so the optimizer cannot cache a TLS address
// across a switch that may resume on another thread.
STACKFULL_NOINLINE ThreadState &currentThreadState() noexcept;

inline bool isNativeContext(ThreadState const &thread, ContextBlock const &block) noexcept {
    return &block == &thread.mainBlock;
}

} // namespace detail
} // namespace coro
} // namespace stackfull
