#pragma once

#include <stackfull/coro/detail/ContextBlock.h>
#include <stackfull/stack/StackAllocator.h>

#include <cstddef>
#include <cstdint>

namespace stackfull {
namespace coro {
namespace detail {

// Where the control records land inside a freshly allocated stack:
//
//   top   ┬─ Entry<F>          (the body, type-erased)
//         ├─ ContextBlock      (16-byte aligned)
//         ├─ stackTop          → make_fcontext; usable stack grows down from here
//         │  ...
//   base  ┴─ (guard page below, outside the view)
//
// Both records sit *above* the stack pointer, so a stack overflow can never
// reach them: it runs into the guard page at the bottom instead.
struct StackLayout {
    void *block = nullptr;
    void *entry = nullptr;
    void *stackTop = nullptr;
    std::size_t usableSize = 0;
    bool fits = false;
};

// A stack smaller than this after carving is refused: make_fcontext itself
// needs ~0xb0 bytes, the entry trampoline and any real body need far more.
constexpr std::size_t kMinUsableStack = 4096;

inline std::uintptr_t alignDown(std::uintptr_t const value, std::size_t const alignment) noexcept {
    return value & ~(static_cast<std::uintptr_t>(alignment) - 1);
}

// `blockSize` / `blockAlign` describe the control record: ContextBlock itself
// for a plain coroutine, or a derived record (the scheduler's Task).
inline StackLayout carveStack(stack::StackView const &stack, std::size_t const entrySize,
                              std::size_t const entryAlign, std::size_t const blockSize,
                              std::size_t const blockAlign) noexcept {
    std::size_t const stackAlign = 16;
    std::size_t const align = entryAlign > stackAlign ? entryAlign : stackAlign;
    std::size_t const recordAlign = blockAlign > stackAlign ? blockAlign : stackAlign;

    auto const top = reinterpret_cast<std::uintptr_t>(stack::topOf(stack));
    auto const base = reinterpret_cast<std::uintptr_t>(stack.base);

    std::uintptr_t const entryAddr = alignDown(top - entrySize, align);
    std::uintptr_t const blockAddr = alignDown(entryAddr - blockSize, recordAlign);

    StackLayout layout;
    layout.fits = entryAddr <= top and blockAddr < entryAddr and blockAddr > base and
                  (blockAddr - base) >= kMinUsableStack;
    if (not layout.fits) {
        return layout;
    }
    layout.block = reinterpret_cast<void *>(blockAddr);
    layout.entry = reinterpret_cast<void *>(entryAddr);
    layout.stackTop = reinterpret_cast<void *>(blockAddr);
    layout.usableSize = static_cast<std::size_t>(blockAddr - base);
    return layout;
}

inline StackLayout carveStack(stack::StackView const &stack, std::size_t const entrySize,
                              std::size_t const entryAlign) noexcept {
    return carveStack(stack, entrySize, entryAlign, sizeof(ContextBlock), alignof(ContextBlock));
}

} // namespace detail
} // namespace coro
} // namespace stackfull
