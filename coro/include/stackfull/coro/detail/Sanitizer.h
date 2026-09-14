#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/detail/ContextBlock.h>

#include <cstddef>

#if STACKFULL_HAS_ASAN
extern "C" {
void __sanitizer_start_switch_fiber(void **fakeStackSave, void const *bottom, std::size_t size);
void __sanitizer_finish_switch_fiber(void *fakeStackSave, void const **bottomOld, std::size_t *sizeOld);
}
#endif

#if STACKFULL_HAS_TSAN
extern "C" {
void *__tsan_get_current_fiber();
void *__tsan_create_fiber(unsigned flags);
void __tsan_destroy_fiber(void *fiber);
void __tsan_switch_to_fiber(void *fiber, unsigned flags);
}
#endif

namespace stackfull {
namespace coro {
namespace detail {

// Called on the departing side, right before the jump. `fromWillResume == false`
// tells ASan the departing fiber is gone for good so its fake stack is freed.
STACKFULL_ALWAYS_INLINE void asanStartSwitch(ContextBlock &from, ContextBlock const &to, bool const fromWillResume) noexcept {
#if STACKFULL_HAS_ASAN
    __sanitizer_start_switch_fiber(fromWillResume ? &from.asanFakeStack : nullptr, to.asanBottom, to.asanSize);
#else
    static_cast<void>(from);
    static_cast<void>(to);
    static_cast<void>(fromWillResume);
#endif
}

// Called on the arriving side. ASan reports the bounds of the stack we came
// from; a thread's native stack has no other way to learn them, so record them
// the first time that thread switches away.
STACKFULL_ALWAYS_INLINE void asanFinishSwitch(ContextBlock &self, ContextBlock &prev) noexcept {
#if STACKFULL_HAS_ASAN
    void const *bottomOld = nullptr;
    std::size_t sizeOld = 0;
    __sanitizer_finish_switch_fiber(self.asanFakeStack, &bottomOld, &sizeOld);
    if (prev.asanBottom == nullptr) {
        prev.asanBottom = bottomOld;
        prev.asanSize = sizeOld;
    }
#else
    static_cast<void>(self);
    static_cast<void>(prev);
#endif
}

// --- ThreadSanitizer --------------------------------------------------------
// TSan tracks one shadow stack per "fiber"; without these hooks every stack
// switch looks like a corrupted call stack. flags = 0 makes the switch a
// synchronization point, which matches the happens-before our protocol
// establishes between the departing and arriving context.

inline void tsanCreateFiber(ContextBlock &block) noexcept {
#if STACKFULL_HAS_TSAN
    block.tsanFiber = __tsan_create_fiber(0);
#else
    static_cast<void>(block);
#endif
}

inline void tsanAdoptCurrentFiber(ContextBlock &block) noexcept {
#if STACKFULL_HAS_TSAN
    block.tsanFiber = __tsan_get_current_fiber();
#else
    static_cast<void>(block);
#endif
}

// Only for a context that is not running (its stack is about to be freed).
inline void tsanDestroyFiber(ContextBlock &block) noexcept {
#if STACKFULL_HAS_TSAN
    if (block.tsanFiber != nullptr) {
        __tsan_destroy_fiber(block.tsanFiber);
        block.tsanFiber = nullptr;
    }
#else
    static_cast<void>(block);
#endif
}

STACKFULL_ALWAYS_INLINE void tsanSwitchTo(ContextBlock const &to) noexcept {
#if STACKFULL_HAS_TSAN
    __tsan_switch_to_fiber(to.tsanFiber, 0);
#else
    static_cast<void>(to);
#endif
}

} // namespace detail
} // namespace coro
} // namespace stackfull
