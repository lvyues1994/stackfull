#include <stackfull/coro/Coroutine.h>

#include "Lifecycle.h"

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#include <utility>
#endif

namespace stackfull {
namespace coro {

// Only the cold paths live here; resume()/yield() are inline in the header.

Coroutine &Coroutine::operator=(Coroutine &&other) noexcept {
    if (this != &other) {
        if (block != nullptr) {
            detail::destroyBlock(*block);
        }
        block = other.block;
        other.block = nullptr;
    }
    return *this;
}

Coroutine::~Coroutine() {
    if (block != nullptr) {
        detail::destroyBlock(*block);
    }
}

#if STACKFULL_HAS_EXCEPTIONS
void detail::rethrowEscaped(ContextBlock &block) {
    std::exception_ptr const escaped = std::move(block.exception);
    block.exception = nullptr;
    std::rethrow_exception(escaped);
}
#endif

} // namespace coro
} // namespace stackfull
