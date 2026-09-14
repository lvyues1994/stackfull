# L4b — readiness-based asynchronous IO on top of the scheduler.

add_library(stackfull_io STATIC
    include/stackfull/io/Fd.h
    include/stackfull/io/Poller.h
    include/stackfull/io/Registration.h
    include/stackfull/io/Async.h
    include/stackfull/io/Tcp.h
    src/Fd.cpp
    src/Registration.cpp
    src/EpollPoller.cpp
    src/PollPoller.cpp
    src/Async.cpp
    src/Tcp.cpp)
add_library(stackfull::io ALIAS stackfull_io)

target_include_directories(stackfull_io PUBLIC include)
target_link_libraries(stackfull_io
    PUBLIC stackfull::options stackfull::sched stackfull::sync
    PRIVATE stackfull_warnings)

target_link_libraries(stackfull INTERFACE stackfull::io)
