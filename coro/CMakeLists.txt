# L2 — Context / Coroutine.

add_library(stackfull_coro STATIC
    include/stackfull/coro/Config.h
    include/stackfull/coro/Fatal.h
    include/stackfull/coro/CoroutineState.h
    include/stackfull/coro/ForcedUnwind.h
    include/stackfull/coro/Coroutine.h
    include/stackfull/coro/detail/EhGlobals.h
    include/stackfull/coro/detail/ContextBlock.h
    include/stackfull/coro/detail/ThreadState.h
    include/stackfull/coro/detail/Sanitizer.h
    include/stackfull/coro/detail/Switch.h
    include/stackfull/coro/detail/StackLayout.h
    src/Fatal.cpp
    src/EhGlobals.cpp
    src/ThreadState.cpp
    src/Lifecycle.h
    src/Lifecycle.cpp
    src/Coroutine.cpp)
add_library(stackfull::coro ALIAS stackfull_coro)

target_include_directories(stackfull_coro PUBLIC include)

if(STACKFULL_TLS_BACKEND STREQUAL "THREAD_LOCAL")
    target_compile_definitions(stackfull_coro PRIVATE STACKFULL_TLS_BACKEND_THREAD_LOCAL=1)
elseif(STACKFULL_TLS_BACKEND STREQUAL "PTHREAD_KEY")
    target_compile_definitions(stackfull_coro PRIVATE STACKFULL_TLS_BACKEND_PTHREAD_KEY=1)
elseif(NOT STACKFULL_TLS_BACKEND STREQUAL "AUTO")
    message(FATAL_ERROR "STACKFULL_TLS_BACKEND must be AUTO, THREAD_LOCAL or PTHREAD_KEY")
endif()

find_package(Threads REQUIRED)
target_link_libraries(stackfull_coro
    PUBLIC stackfull::options stackfull::fcontext stackfull::stack
    PRIVATE stackfull_warnings Threads::Threads)

# Umbrella target: consumers link this one.
add_library(stackfull INTERFACE)
add_library(stackfull::stackfull ALIAS stackfull)
target_link_libraries(stackfull INTERFACE stackfull::coro)
