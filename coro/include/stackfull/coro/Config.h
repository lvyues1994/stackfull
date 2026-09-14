#pragma once

// Build-mode switches for the coroutine layer. CMake normally supplies the
// STACKFULL_* values as PUBLIC compile definitions so every translation unit —
// library and consumer alike — agrees on the control block layout. The
// fallbacks below serve non-CMake consumers.

// --- C++ exceptions -------------------------------------------------------
#ifndef STACKFULL_HAS_EXCEPTIONS
#if defined(__cpp_exceptions) || defined(__EXCEPTIONS)
#define STACKFULL_HAS_EXCEPTIONS 1
#else
#define STACKFULL_HAS_EXCEPTIONS 0
#endif
#endif

// --- Itanium ABI exception-globals swap ------------------------------------
// The per-thread "caught exceptions" chain must follow a coroutine when a catch
// block spans a yield or when the coroutine resumes on another thread.
#ifndef STACKFULL_SWAP_EH_GLOBALS
#define STACKFULL_SWAP_EH_GLOBALS STACKFULL_HAS_EXCEPTIONS
#endif
#if STACKFULL_SWAP_EH_GLOBALS && !STACKFULL_HAS_EXCEPTIONS
#error "STACKFULL_SWAP_EH_GLOBALS requires STACKFULL_HAS_EXCEPTIONS"
#endif

// --- AddressSanitizer fiber annotations ------------------------------------
#ifndef STACKFULL_HAS_ASAN
#if defined(__SANITIZE_ADDRESS__)
#define STACKFULL_HAS_ASAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define STACKFULL_HAS_ASAN 1
#else
#define STACKFULL_HAS_ASAN 0
#endif
#else
#define STACKFULL_HAS_ASAN 0
#endif
#endif

// --- TLS access model ------------------------------------------------------
// Under -fPIC the default (global-dynamic) model routes every thread_local
// access through __tls_get_addr. initial-exec is a single %fs-relative load
// and is valid for static libraries and shared objects loaded at startup.
// Override with -DSTACKFULL_TLS_MODEL="global-dynamic" for a dlopen()-ed
// shared build on a platform without static TLS surplus.
#ifndef STACKFULL_TLS_MODEL
#define STACKFULL_TLS_MODEL "initial-exec"
#endif
#define STACKFULL_TLS_MODEL_ATTR __attribute__((tls_model(STACKFULL_TLS_MODEL)))

// --- Compiler helpers -----------------------------------------------------
#define STACKFULL_NOINLINE __attribute__((noinline))
#define STACKFULL_NODISCARD __attribute__((warn_unused_result))
// The switch path must be inlined into its caller: an out-of-line wrapper
// adds one mispredicted `ret` per switch (the return stack buffer belongs to
// the other stack after a switch). Compilers judge these functions too big
// to inline on their own.
#define STACKFULL_ALWAYS_INLINE inline __attribute__((always_inline))
