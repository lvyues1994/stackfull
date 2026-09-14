# L3 — M:N scheduler.

set(STACKFULL_SCHED_QUEUE "BWOS" CACHE STRING
    "Per-worker run queue: BWOS (block-based work stealing) | RING (Go-style reference)")
set_property(CACHE STACKFULL_SCHED_QUEUE PROPERTY STRINGS BWOS RING)

add_library(stackfull_sched STATIC
    include/stackfull/sched/SchedulerOptions.h
    include/stackfull/sched/Scheduler.h
    include/stackfull/sched/JoinHandle.h
    include/stackfull/sched/WakeToken.h
    include/stackfull/sched/ThisTask.h
    include/stackfull/sched/Parker.h
    include/stackfull/sched/Driver.h
    include/stackfull/sched/Sleep.h
    include/stackfull/sched/detail/Task.h
    include/stackfull/sched/detail/MpscQueue.h
    include/stackfull/sched/detail/SchedulerCore.h
    include/stackfull/sched/detail/Worker.h
    include/stackfull/sched/detail/Runtime.h
    include/stackfull/sched/detail/TaskFactory.h
    include/stackfull/sched/detail/JoinState.h
    include/stackfull/sched/detail/TimerQueue.h
    src/Unwind.h
    src/Parker.cpp
    src/TimerQueue.cpp
    src/Task.cpp
    src/Worker.cpp
    src/SchedulerCore.cpp
    src/Scheduler.cpp
    src/JoinHandle.cpp
    src/WakeToken.cpp)
add_library(stackfull::sched ALIAS stackfull_sched)

target_include_directories(stackfull_sched PUBLIC include)

if(STACKFULL_SCHED_QUEUE STREQUAL "RING")
    # Changes the Worker layout: consumers must see it too.
    target_compile_definitions(stackfull_sched PUBLIC STACKFULL_SCHED_QUEUE_RING=1)
elseif(NOT STACKFULL_SCHED_QUEUE STREQUAL "BWOS")
    message(FATAL_ERROR "STACKFULL_SCHED_QUEUE must be BWOS or RING")
endif()

find_package(Threads REQUIRED)
target_link_libraries(stackfull_sched
    PUBLIC stackfull::options stackfull::coro stackfull::queue Threads::Threads
    PRIVATE stackfull_warnings)

target_link_libraries(stackfull INTERFACE stackfull::sched)
