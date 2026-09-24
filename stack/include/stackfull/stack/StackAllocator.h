#pragma once

#include <cstddef>
#include <system_error>

namespace stackfull {
namespace stack {

// The usable part of a coroutine stack: [base, base + size). Guard pages, if
// any, live *outside* this range so callers can never touch them by accident.
// `base + size` is the top of the stack and is always 16-byte aligned.
struct StackView {
    void *base = nullptr;
    std::size_t size = 0;
};

inline void *topOf(StackView const &stack) noexcept {
    return static_cast<char *>(stack.base) + stack.size;
}

inline bool isEmpty(StackView const &stack) noexcept {
    return stack.base == nullptr;
}

// Result of an allocation attempt. On failure `stack` is empty and `error`
// carries the reason (errno through std::generic_category(), or
// std::errc::invalid_argument for a size that cannot be honoured).
struct StackAllocation {
    StackView stack{};
    std::error_code error{};

    explicit operator bool() const noexcept { return not error; }
};

// Cold-path strategy for obtaining coroutine stacks. Every implementation must
// be safe to call from any thread and must accept a `deallocate` from a thread
// other than the one that allocated (coroutines may migrate).
struct StackAllocator {
    virtual ~StackAllocator() = default;

    // `size` is the requested usable size in bytes; implementations may round
    // it up (typically to the page size) and report the real size in the view.
    virtual StackAllocation allocate(std::size_t size) noexcept = 0;

    virtual void deallocate(StackView const &stack) noexcept = 0;

    // Allocates up to `count` stacks of `size` into `out`; returns how many.
    // Implementations may batch the system calls.
    virtual std::size_t allocateMany(std::size_t const size, StackView *const out, std::size_t const count) noexcept {
        std::size_t done = 0;
        for (; done < count; ++done) {
            StackAllocation const allocation = allocate(size);
            if (not allocation) {
                break;
            }
            out[done] = allocation.stack;
        }
        return done;
    }

    // Sets aside `count` stacks of `size` so that later allocations of that
    // size need no system call (a startup prewarm); returns how many are now
    // held for that size. The default holds none.
    virtual std::size_t reserve(std::size_t const size, std::size_t const count) noexcept {
        static_cast<void>(size);
        static_cast<void>(count);
        return 0;
    }
};

} // namespace stack
} // namespace stackfull
