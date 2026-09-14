#pragma once

#include <stackfull/coro/Config.h>

namespace stackfull {
namespace coro {
namespace detail {

#if STACKFULL_SWAP_EH_GLOBALS

// Mirror of the Itanium C++ ABI `__cxa_eh_globals` record kept per thread by
// libstdc++ and libc++abi. Both runtimes agree on this layout; the ARM EHABI
// unwinder (32-bit ARM without DWARF EH) appends one more pointer.
struct EhGlobals {
    void *caughtExceptions = nullptr;
    unsigned int uncaughtExceptions = 0;
#if defined(__ARM_EABI_UNWINDER__) || (defined(__arm__) && !defined(__ARM_DWARF_EH__))
    void *propagatingExceptions = nullptr;
#endif
};

// The calling thread's live record. Out of line and never inlined: the ABI
// declares __cxa_get_globals() `const`, which would let the optimizer hoist it
// across a switch — and after a migration the result belongs to another thread.
STACKFULL_NOINLINE EhGlobals &currentEhGlobals() noexcept;

#else

struct EhGlobals {};

#endif

} // namespace detail
} // namespace coro
} // namespace stackfull
