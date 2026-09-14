cmake_minimum_required(VERSION 3.21)

project(stackfull
    VERSION 0.1.0
    DESCRIPTION "High-performance stackful coroutines on boost.fcontext (C++14)"
    LANGUAGES CXX ASM)

# ---------------------------------------------------------------------------
# Project-wide policy
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(STACKFULL_EXCEPTIONS
    "Build with C++ exceptions. OFF adds -fno-exceptions and disables exception \
transport / forced unwind; the public API is identical in both modes." ON)

set(STACKFULL_TLS_BACKEND "AUTO" CACHE STRING
    "How the per-thread state is located: AUTO | THREAD_LOCAL | PTHREAD_KEY. \
AUTO picks PTHREAD_KEY on Android < API 29 (emulated TLS), THREAD_LOCAL elsewhere.")
set_property(CACHE STACKFULL_TLS_BACKEND PROPERTY STRINGS AUTO THREAD_LOCAL PTHREAD_KEY)

option(STACKFULL_SWAP_EH_GLOBALS
    "Swap the Itanium C++ ABI per-thread exception globals on every switch so \
catch blocks may span a yield and coroutines may migrate between threads." ON)

set(STACKFULL_SANITIZE "" CACHE STRING
    "Developer sanitizer profile for this build tree: '' | address | thread | undefined")
set_property(CACHE STACKFULL_SANITIZE PROPERTY STRINGS "" address thread undefined)

option(STACKFULL_BUILD_TESTS "Build unit tests" ${PROJECT_IS_TOP_LEVEL})
option(STACKFULL_BUILD_BENCH "Build micro benchmarks" ${PROJECT_IS_TOP_LEVEL})

# ---------------------------------------------------------------------------
# Shared usage requirements (warnings, exception mode, sanitizers)
# ---------------------------------------------------------------------------
include(cmake/StackfullOptions.cmake)

# ---------------------------------------------------------------------------
# Modules — one directory, one target, mirroring the layer diagram
# ---------------------------------------------------------------------------
add_subdirectory(queue)      # lock-free queues (header-only), used by stack and sched
add_subdirectory(fcontext)   # L0: vendored boost.fcontext assembly + C++ declarations
add_subdirectory(stack)      # L1: StackAllocator interface, mmap + pooled implementations
add_subdirectory(coro)       # L2: Context / Coroutine
add_subdirectory(sched)      # L3: M:N scheduler
add_subdirectory(sync)       # L4a: task-aware synchronization primitives
add_subdirectory(io)         # L4b: poller-driven asynchronous IO

if(STACKFULL_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(STACKFULL_BUILD_BENCH)
    add_subdirectory(bench)
endif()
