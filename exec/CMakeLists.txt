# stackfull::exec — tasks and schedulers in lexec's sender/receiver model.
# Header-only, C++17 (the rest of stackfull stays C++14).

# lexec probes whether <execution> runs on TBB (programs then link TBB) with
# CMAKE_CXX_STANDARD; at the project's 14 the parallel backend never shows
# and the probe says no. This directory and lexec's below it are C++17.
set(CMAKE_CXX_STANDARD 17)

include(FetchContent)
FetchContent_Declare(lexec
    GIT_REPOSITORY https://github.com/lvyues1994/lexec.git
    GIT_TAG a2568cc5bf14f69addc7d0e04b1e5ed5c0b2173b)
FetchContent_MakeAvailable(lexec)

add_library(stackfull_exec INTERFACE)
add_library(stackfull::exec ALIAS stackfull_exec)

target_include_directories(stackfull_exec INTERFACE include)
target_compile_features(stackfull_exec INTERFACE cxx_std_17)
target_link_libraries(stackfull_exec INTERFACE stackfull::sched stackfull::sync stackfull::io lexec::lexec)
