# L1 — coroutine stack allocation strategies.

add_library(stackfull_stack STATIC
    include/stackfull/stack/StackAllocator.h
    include/stackfull/stack/MmapStackAllocator.h
    include/stackfull/stack/PooledStackAllocator.h
    include/stackfull/stack/DefaultStackAllocator.h
    src/MmapStackAllocator.cpp
    src/PooledStackAllocator.cpp
    src/DefaultStackAllocator.cpp)
add_library(stackfull::stack ALIAS stackfull_stack)

target_include_directories(stackfull_stack PUBLIC include)

find_package(Threads REQUIRED)
target_link_libraries(stackfull_stack
    PUBLIC stackfull::options
    PRIVATE stackfull_warnings Threads::Threads)
