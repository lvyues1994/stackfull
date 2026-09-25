# L4b — readiness-based asynchronous IO on top of the scheduler.

add_library(stackfull_io STATIC
    include/stackfull/io/Fd.h
    include/stackfull/io/Poller.h
    include/stackfull/io/Registration.h
    include/stackfull/io/Async.h
    include/stackfull/io/SocketAddress.h
    include/stackfull/io/Resolve.h
    include/stackfull/io/StreamSocket.h
    include/stackfull/io/Tcp.h
    include/stackfull/io/TcpStream.h
    include/stackfull/io/UnixStream.h
    include/stackfull/io/UdpSocket.h
    src/Fd.cpp
    src/Registration.cpp
    src/EpollPoller.cpp
    src/PollPoller.cpp
    src/Async.cpp
    src/Socket.h
    src/Socket.cpp
    src/SocketAddress.cpp
    src/Resolve.cpp
    src/StreamSocket.cpp
    src/Tcp.cpp
    src/TcpStream.cpp
    src/UnixStream.cpp
    src/UdpSocket.cpp)
add_library(stackfull::io ALIAS stackfull_io)

target_include_directories(stackfull_io PUBLIC include)
target_link_libraries(stackfull_io
    PUBLIC stackfull::options stackfull::sched stackfull::sync
    PRIVATE stackfull_warnings)

target_link_libraries(stackfull INTERFACE stackfull::io)
