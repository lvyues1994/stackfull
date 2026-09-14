# Application-level entry points: defaultScheduler(), go(), async(), blockOn().

add_library(stackfull_runtime STATIC
    include/stackfull/runtime/Runtime.h
    src/Runtime.cpp)
add_library(stackfull::runtime ALIAS stackfull_runtime)

target_include_directories(stackfull_runtime PUBLIC include)
target_link_libraries(stackfull_runtime
    PUBLIC stackfull::options stackfull::sched stackfull::sync
    PRIVATE stackfull_warnings)

target_link_libraries(stackfull INTERFACE stackfull::runtime)
