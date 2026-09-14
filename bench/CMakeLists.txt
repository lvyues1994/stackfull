add_executable(stackfull_switch_bench SwitchBench.cpp)
target_link_libraries(stackfull_switch_bench PRIVATE stackfull::coro stackfull_warnings)

find_package(Threads REQUIRED)
add_executable(stackfull_queue_bench QueueBench.cpp)
target_link_libraries(stackfull_queue_bench PRIVATE stackfull::queue Threads::Threads stackfull_warnings)

add_executable(stackfull_sched_bench SchedBench.cpp)
target_link_libraries(stackfull_sched_bench PRIVATE stackfull::sched stackfull_warnings)
