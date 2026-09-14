#include <stackfull/fcontext/Fcontext.h>

#include <type_traits>

// The assembly returns transfer_t in two registers (x86_64 SysV: rax:rdx,
// AArch64: x0:x1) or through the hidden result pointer (AAPCS32). Both require
// the C++ view of the struct to be exactly two pointer-sized words.
static_assert(sizeof(stackfull::fcontext::transfer_t) == 2 * sizeof(void *),
              "transfer_t must be two machine words");
static_assert(std::is_trivial<stackfull::fcontext::transfer_t>::value,
              "transfer_t must be trivially copyable for the C ABI");
static_assert(std::is_standard_layout<stackfull::fcontext::transfer_t>::value,
              "transfer_t must be standard layout");
