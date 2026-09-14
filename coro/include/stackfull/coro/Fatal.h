#pragma once

#include <cassert>

namespace stackfull {
namespace coro {

// Contract violations (resuming a running coroutine, yielding from a thread's
// native context, ...) are not recoverable: print `message` and abort. Kept
// out of line so the check at the call site stays a single compare.
[[noreturn]] void fatal(char const *message) noexcept;

} // namespace coro
} // namespace stackfull

// Always-on precondition check for cheap API misuse detection.
#define STACKFULL_CHECK(condition, message)                                                          \
    do {                                                                                             \
        if (!(condition)) {                                                                          \
            ::stackfull::coro::fatal(message);                                                       \
        }                                                                                            \
    } while (0)

// Debug-only internal invariant.
#define STACKFULL_ASSERT(condition) assert(condition)
