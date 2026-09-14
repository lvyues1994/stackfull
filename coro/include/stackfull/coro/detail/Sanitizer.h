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

} // namespace detail
} // namespace coro
} // namespace stackfull
