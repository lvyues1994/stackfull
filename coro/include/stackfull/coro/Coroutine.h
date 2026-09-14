#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/CoroutineState.h>
#include <stackfull/coro/Fatal.h>
#include <stackfull/coro/detail/ContextBlock.h>
#include <stackfull/coro/detail/StackLayout.h>
#include <stackfull/coro/detail/Switch.h>
#include <stackfull/coro/detail/ThreadState.h>
#include <stackfull/fcontext/Fcontext.h>
#include <stackfull/stack/DefaultStackAllocator.h>
#include <stackfull/stack/StackAllocator.h>

#include <cstddef>
#include <new>
#include <system_error>
#include <type_traits>
#include <utility>

namespace stackfull {
namespace coro {

constexpr std::size_t kDefaultStackSize = std::size_t{128} * 1024;

struct CoroutineOptions {
    // Total stack reservation. The control records (~200 bytes) are carved
    // from its top; the remainder is the body's stack.
    std::size_t stackSize = kDefaultStackSize;
    // Borrowed; nullptr selects stack::defaultStackAllocator().
    stack::StackAllocator *allocator = nullptr;
};

struct CoroutineCreation;

template <class F>
CoroutineCreation makeCoroutine(F &&body, CoroutineOptions const &options = CoroutineOptions{});

// Asymmetric coroutine handle: move-only, pointer-sized, owns one stack.
//
//   resume()  runs the body until it yields or returns. Any thread may call it
//             as long as calls are sequenced; the coroutine continues on the
//             calling thread.
//   yield()   from inside the body, returns control to whoever resumed us.
//
// With exceptions enabled, an exception escaping the body is rethrown from the
// resume() that observed it, and destroying a suspended handle unwinds the
// body's stack (see ForcedUnwind.h). Without exceptions, destroying a
// suspended handle releases the stack without running its destructors; use
// requestStop()/stopRequested() for cooperative shutdown in both modes.
struct Coroutine {
    Coroutine() noexcept = default;
    Coroutine(Coroutine const &) = delete;
    Coroutine &operator=(Coroutine const &) = delete;
    Coroutine(Coroutine &&other) noexcept : block(other.block) { other.block = nullptr; }
    Coroutine &operator=(Coroutine &&other) noexcept;
    ~Coroutine();

    explicit operator bool() const noexcept { return block != nullptr; }

    // Precondition for the following: non-empty handle.
    CoroutineState state() const noexcept { return block->state; }
    bool isDone() const noexcept { return block->state == CoroutineState::Done; }

    // Precondition: state() is Created or Suspended, and the caller is not
    // this coroutine. Postcondition: Suspended or Done.
    //
    // resume() and yield() are inline on purpose: after a stack switch every
    // `ret` that unwinds a pre-switch frame mispredicts (the return stack
    // buffer belongs to the other stack), so an out-of-line wrapper would add
    // one guaranteed mispredict (~5 ns) to every switch.
    void resume();

    // Cooperative cancellation flag observed by stopRequested() in the body.
    void requestStop() noexcept { block->stopRequested = true; }

    // --- callable only from inside a coroutine body ------------------------
    static void yield();
    static bool stopRequested() noexcept;
    static bool isInsideCoroutine() noexcept;

private:
    explicit Coroutine(detail::ContextBlock *const block_) noexcept : block(block_) {}

    template <class F>
    friend CoroutineCreation makeCoroutine(F &&body, CoroutineOptions const &options);

    detail::ContextBlock *block = nullptr;
};

// Result of makeCoroutine. Identical in both exception modes: a failed stack
// allocation or an unusable stackSize is reported here, never thrown.
struct CoroutineCreation {
    Coroutine coroutine;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

namespace detail {

// First-entry trampoline handed to make_fcontext (Lifecycle.cpp).
void contextEntry(fcontext::transfer_t transfer);

#if STACKFULL_HAS_EXCEPTIONS
// Moves the parked exception out of the block and rethrows it (Coroutine.cpp).
[[noreturn]] void rethrowEscaped(ContextBlock &block);
#endif

} // namespace detail

// ---------------------------------------------------------------------------
// Hot path (see the note on resume() above).
// ---------------------------------------------------------------------------

inline void Coroutine::resume() {
    STACKFULL_CHECK(block != nullptr, "stackfull: resume() on an empty handle");
    STACKFULL_CHECK(block->state == CoroutineState::Created or block->state == CoroutineState::Suspended,
                    "stackfull: resume() requires a Created or Suspended coroutine");

    ThreadState &thread = detail::currentThreadState();
    detail::ContextBlock &from = *thread.current;
    block->resumer = &from;
    detail::switchTo(from, *block);

#if STACKFULL_HAS_EXCEPTIONS
    if (block->exception) {
        detail::rethrowEscaped(*block);
    }
#endif
}

inline void Coroutine::yield() {
    ThreadState &thread = detail::currentThreadState();
    detail::ContextBlock &self = *thread.current;
    STACKFULL_CHECK(not detail::isNativeContext(thread, self), "stackfull: yield() outside a coroutine");
    detail::switchTo(self, *self.resumer);
}

inline bool Coroutine::stopRequested() noexcept {
    return detail::currentThreadState().current->stopRequested;
}

inline bool Coroutine::isInsideCoroutine() noexcept {
    ThreadState const &thread = detail::currentThreadState();
    return not detail::isNativeContext(thread, *thread.current);
}

// ---------------------------------------------------------------------------
namespace detail {

template <class Body>
struct EntryImpl final : Entry {
    template <class F>
    explicit EntryImpl(F &&body_) : body(std::forward<F>(body_)) {}

    void run() override { body(); }

    Body body;
};

// Releases a freshly allocated stack unless construction completes.
struct StackReleaseGuard {
    stack::StackAllocator *allocator;
    stack::StackView stack;

    ~StackReleaseGuard() {
        if (allocator != nullptr) {
            allocator->deallocate(stack);
        }
    }
    void release() noexcept { allocator = nullptr; }
};

} // namespace detail

template <class F>
CoroutineCreation makeCoroutine(F &&body, CoroutineOptions const &options) {
    using Body = typename std::decay<F>::type;
    using Impl = detail::EntryImpl<Body>;

    stack::StackAllocator &allocator =
        options.allocator != nullptr ? *options.allocator : stack::defaultStackAllocator();
    stack::StackAllocation const allocation = allocator.allocate(options.stackSize);
    if (not allocation) {
        return CoroutineCreation{Coroutine{}, allocation.error};
    }

    detail::StackReleaseGuard guard{&allocator, allocation.stack};
    detail::StackLayout const layout = detail::carveStack(allocation.stack, sizeof(Impl), alignof(Impl));
    if (not layout.fits) {
        return CoroutineCreation{Coroutine{}, std::make_error_code(std::errc::invalid_argument)};
    }

    detail::Entry *const entry = ::new (layout.entry) Impl{std::forward<F>(body)};
    auto *const block = ::new (layout.block) detail::ContextBlock{};
    block->entry = entry;
    block->stack = allocation.stack;
    block->allocator = &allocator;
#if STACKFULL_HAS_ASAN
    block->asanBottom = allocation.stack.base;
    block->asanSize = allocation.stack.size;
#endif
    block->fctx = fcontext::make(layout.stackTop, layout.usableSize, &detail::contextEntry);

    guard.release();
    return CoroutineCreation{Coroutine{block}, std::error_code{}};
}

} // namespace coro
} // namespace stackfull
