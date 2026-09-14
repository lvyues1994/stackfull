#pragma once

#include <stackfull/coro/detail/ContextBlock.h>

// Private to the coro module: birth and death of a coroutine's ContextBlock.

namespace stackfull {
namespace coro {
namespace detail {

// Runs the body's destructor if it is still alive.
void destroyEntry(ContextBlock &block) noexcept;

// Tears down a block created by makeCoroutine and releases its stack. A
// Suspended block is force-unwound first when exceptions are available.
// Precondition: `block` is not the running context.
void destroyBlock(ContextBlock &block);

} // namespace detail
} // namespace coro
} // namespace stackfull
