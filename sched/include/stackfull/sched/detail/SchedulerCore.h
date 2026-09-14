#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/queue/BbqQueue.h>
#include <stackfull/queue/BwosQueue.h>
#include <stackfull/queue/RingQueue.h>
#include <stackfull/sched/SchedulerOptions.h>
#include <stackfull/sched/detail/Task.h>
#include <stackfull/stack/StackAllocator.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace stackfull {
namespace sched {
namespace detail {

struct Worker;

// Per-worker run queue. BWoS by default; the Go-style ring is selectable at
// build time (STACKFULL_SCHED_QUEUE=RING) for head-to-head measurements.
#if defined(STACKFULL_SCHED_QUEUE_RING)
using LocalQueue = queue::RingQueue<Task *, 256>;
#else
using LocalQueue = queue::BwosQueue<Task *, 8, 32>;
#endif

// Global MPMC queue: external spawns/wakes, placement onto idle workers, and
// local-queue overflow. Also reused for the free list of slab indices.
using InjectionQueue = queue::BbqQueue<Task *, 32, 4096>;
using SlotQueue = queue::BbqQueue<std::uint32_t, 32, 4096>;

constexpr std::size_t kMaxWorkers = 64;

// One entry of the wake-token slab. `pins` counts wakers currently holding
// the Task pointer; release waits for it to drop to zero before the stack
// (and the Task inside it) is freed.
struct TaskSlot {
    std::atomic<Task *> task{nullptr};
    std::atomic<std::uint32_t> generation{0};
    std::atomic<std::uint32_t> pins{0};
};

// Shared state of one scheduler. Hot operations (schedule / wake / inject)
// are inline in Runtime.h; everything else lives in SchedulerCore.cpp.
struct SchedulerCore {
    explicit SchedulerCore(SchedulerOptions const &options);
    ~SchedulerCore();
    SchedulerCore(SchedulerCore const &) = delete;
    SchedulerCore &operator=(SchedulerCore const &) = delete;

    // --- hot, inline (Runtime.h) ------------------------------------------
    void schedule(Task &task) noexcept;
    void wake(Task &task) noexcept;
    void inject(Task &task) noexcept;
    Worker *currentWorker() noexcept;
    bool hasIdleWorkers() const noexcept;

    // --- cold (SchedulerCore.cpp) -------------------------------------------
    // Unpark one idle worker unless a searching worker will pick the work up.
    void notifyIdleWorker() noexcept;
    // Unpark a specific worker if it is idle (pinned wakeups).
    void notifyWorker(Worker &worker) noexcept;
    void markIdle(Worker &worker) noexcept;
    bool clearIdle(Worker &worker) noexcept;
    Task *popInjection() noexcept;
    // Called on the arriving side after a finished task switched away.
    void releaseTask(Task &task) noexcept;
    // Wake every parked task once (cooperative shutdown), from any thread.
    void wakeAllParked() noexcept;

    SchedulerOptions options;
    stack::StackAllocator *allocator = nullptr;
#if STACKFULL_HAS_EXCEPTIONS
    ExceptionSink *exceptionSink = nullptr;
#endif

    std::vector<std::unique_ptr<Worker>> workers;

    InjectionQueue injection;
    std::unique_ptr<TaskSlot[]> slots;
    SlotQueue freeSlots;

    std::atomic<std::uint64_t> idleMask{0};
    std::atomic<std::uint32_t> searching{0};
    std::atomic<std::uint32_t> liveTasks{0};
    std::atomic<std::uint32_t> workersRunning{0};
    std::atomic<bool> stopping{false};
};

} // namespace detail
} // namespace sched
} // namespace stackfull
