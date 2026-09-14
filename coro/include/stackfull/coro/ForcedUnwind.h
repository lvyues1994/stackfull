#pragma once

#include <stackfull/coro/Config.h>

#if STACKFULL_HAS_EXCEPTIONS

namespace stackfull {
namespace coro {

// Thrown *inside* a suspended coroutine (at its yield() call site) when its
// handle is destroyed, so that every object on the coroutine stack is
// destroyed through normal unwinding. Deliberately not derived from
// std::exception: a `catch (std::exception const &)` in user code must not
// swallow it. If you write `catch (...)` inside a coroutine, rethrow.
struct ForcedUnwind {};

} // namespace coro
} // namespace stackfull

#endif
