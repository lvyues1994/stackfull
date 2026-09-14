# Lock-free queues used by the scheduler. Header-only templates.
#
#   BwosQueue  per-worker run queue (single owner + thieves)
#   BbqQueue   global injection queue (MPMC)
#   RingQueue  Go-style reference used as test oracle and benchmark baseline

add_library(stackfull_queue INTERFACE)
add_library(stackfull::queue ALIAS stackfull_queue)

target_include_directories(stackfull_queue INTERFACE include)
target_sources(stackfull_queue INTERFACE
    include/stackfull/queue/detail/PackedCursor.h
    include/stackfull/queue/BwosQueue.h
    include/stackfull/queue/BbqQueue.h
    include/stackfull/queue/RingQueue.h)
target_link_libraries(stackfull_queue INTERFACE stackfull::options)
