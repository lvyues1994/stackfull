#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/CoroutineState.h>
#include <stackfull/coro/detail/EhGlobals.h>
#include <stackfull/fcontext/Fcontext.h>
#include <stackfull/stack/StackAllocator.h>

#include <cstddef>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

namespace stackfull {
namespace coro {

struct ThreadState;

namespace detail {

struct ContextBlock;

// The coroutine body, type-erased. Lives on the coroutine's own stack right
// next to its ContextBlock, so creating a coroutine costs exactly one stack
// allocation and nothing else.
struct Entry {
    virtual ~Entry() = default;
    virtual void run() = 0;
};

// Action the *arriving* context performs on behalf of the context that just
// left, once that context's registers are fully saved. A scheduler uses it to
// publish a suspended task to a run queue: publishing before the switch would
// let another thread resume a half-saved context.
struct PostSwitchHook {
    void (*fn)(ContextBlock &suspended, void *arg) = nullptr;
    void *arg = nullptr;
};

// Per-context record. For a coroutine it sits at the top of the coroutine
// stack; for a thread's native context it is embedded in ThreadState.
struct alignas(16) ContextBlock {
    // Saved fcontext. Meaningful only while this context is not running.
    fcontext::fcontext_t fctx = nullptr;

    // Thread currently executing this context. Valid only while running; a
    // suspended context may be resumed by any thread, which refreshes it.
    ThreadState *thread = nullptr;

    // Context to continue when this one yields or finishes. Rewritten by every
    // resume(), so the resumer always reflects the latest caller.
    ContextBlock *resumer = nullptr;

    // nullptr for a thread's native context and after the body has finished.
    Entry *entry = nullptr;

    // Usable stack owned by this context; empty for a native context.
    stack::StackView stack{};
    stack::StackAllocator *allocator = nullptr;

    PostSwitchHook postSwitch{};
    CoroutineState state = CoroutineState::Created;
    bool stopRequested = false;

#if STACKFULL_HAS_EXCEPTIONS
    // Exception that escaped the body; rethrown by the next resume().
    std::exception_ptr exception{};
    // Snapshot of this context's caught-exception chain while it is not running.
    EhGlobals ehGlobals{};
#endif

#if STACKFULL_HAS_ASAN
    void *asanFakeStack = nullptr;
    void const *asanBottom = nullptr;
    std::size_t asanSize = 0;
#endif
};

} // namespace detail
} // namespace coro
} // namespace stackfull
