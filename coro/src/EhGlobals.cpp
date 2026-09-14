#include <stackfull/coro/detail/EhGlobals.h>

#if STACKFULL_SWAP_EH_GLOBALS

// Itanium C++ ABI, §2.5.4. Declared here rather than through <cxxabi.h>:
// libstdc++ declares it there, libc++abi (Android NDK) exports the symbol but
// leaves it out of the header. The signature matches libstdc++'s declaration
// exactly so both may be visible in one translation unit.
namespace __cxxabiv1 {
struct __cxa_eh_globals;
extern "C" __cxa_eh_globals *__cxa_get_globals() noexcept;
} // namespace __cxxabiv1

namespace stackfull {
namespace coro {
namespace detail {

EhGlobals &currentEhGlobals() noexcept {
    // Opaque to the ABI; EhGlobals mirrors the runtime's definition.
    return *reinterpret_cast<EhGlobals *>(__cxxabiv1::__cxa_get_globals());
}

} // namespace detail
} // namespace coro
} // namespace stackfull

#endif
