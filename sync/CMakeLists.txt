# L4 — task-aware synchronization primitives.

add_library(stackfull_sync STATIC
    include/stackfull/sync/SpinLock.h
    include/stackfull/sync/Semaphore.h
    include/stackfull/sync/Mutex.h
    include/stackfull/sync/ConditionVariable.h
    include/stackfull/sync/WaitGroup.h
    include/stackfull/sync/Channel.h
    include/stackfull/sync/detail/Waiter.h
    src/Waiter.cpp
    src/Semaphore.cpp
    src/ConditionVariable.cpp
    src/WaitGroup.cpp)
add_library(stackfull::sync ALIAS stackfull_sync)

target_include_directories(stackfull_sync PUBLIC include)
target_link_libraries(stackfull_sync
    PUBLIC stackfull::options stackfull::sched
    PRIVATE stackfull_warnings)

target_link_libraries(stackfull INTERFACE stackfull::sync)
