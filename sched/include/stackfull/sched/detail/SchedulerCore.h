#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/queue/BbqQueue.h>
#include <stackfull/queue/BwosQueue.h>
#include <stackfull/queue/RingQueue.h>
#include <stackfull/sched/Driver.h>
#include <stackfull/sched/SchedulerOptions.h>
#include <stackfull/sched/detail/Task.h>
#include <stackfull/sched/detail/TimerQueue.h>
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
    // Injection queue push, then notifyIdleWorker().
    void inject(Task &task) noexcept;
    // Injection queue push alone; the caller wakes a worker for the batch.
    void pushInjection(Task &task) noexcept;
    Worker *currentWorker() noexcept;
    bool hasIdleWorkers() const noexcept;
    // Racy; true means a pop may succeed. Scheduling heuristics only.
    bool hasInjectedWork() const noexcept;

    // --- cold (SchedulerCore.cpp) -------------------------------------------
    // Unpark one idle worker unless a searching worker will pick the work up.
    // The timekeeper is chosen only when it is the sole idle worker, so the
    // timers keep a sleeper.
    void notifyIdleWorker() noexcept;
    // Unpark a specific worker if it is idle (pinned wakeups).
    void notifyWorker(Worker &worker) noexcept;
    void markIdle(Worker &worker) noexcept;
    bool clearIdle(Worker &worker) noexcept;
    Task *popInjection() noexcept;
    // Called on the arriving side after a finished task switched away.
    void releaseTask(Task &task) noexcept;
    // Wake-token slots. `me` is the calling thread's worker, or nullptr on a
    // foreign thread. acquireSlot() fails only when every slot is in use.
    bool acquireSlot(Worker *me, std::uint32_t &slot) noexcept;
    bool tryAcquireSlot(Worker *me, std::uint32_t &slot) noexcept;
    void releaseSlot(Worker *me, std::uint32_t slot) noexcept;
    // Live-task accounting, sharded per worker (inline, Runtime.h).
    void countCreated(Worker *me) noexcept;
    void countFinished(Worker *me) noexcept;
    // Never below the true count; exact while no task starts or ends.
    std::size_t liveTasks() const noexcept;
    // Wake every parked task once (cooperative shutdown), from any thread.
    void wakeAllParked() noexcept;

    // --- timers / driver -----------------------------------------------------
    // Exactly one idle worker at a time sleeps with the earliest timer as its
    // timeout (inside the Driver when there is one); the others sleep
    // indefinitely on their Parker.
    bool tryBecomeTimekeeper(Worker &worker) noexcept;
    void releaseTimekeeper(Worker &worker) noexcept;
    // Unpark a worker wherever it sleeps: its Parker, or the Driver if it is
    // the timekeeper inside Driver::wait().
    void unparkWorker(Worker &worker) noexcept;
    // unparkWorker() for new work, flagging whether a peer worker sent it.
    void handOffTo(Worker &worker) noexcept;
    // Queue a deadline; shortens the timekeeper's sleep if it became earliest.
    void addTimer(TimerEntry &entry) noexcept;
    // Fire what is due; any worker may call this.
    void fireTimers() noexcept;
    // Pending timers with nobody holding the timekeeper role: wake an idle
    // peer, which claims the role on its way back to sleep. Called by a
    // worker about to run tasks, which may keep it away from its own idle
    // loop for a while.
    void ensureTimekeeper() noexcept;

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
    // Tasks created / finished on threads that are not workers.
    std::atomic<std::uint64_t> foreignCreated{0};
    std::atomic<std::uint64_t> foreignFinished{0};
    std::atomic<std::uint32_t> workersRunning{0};
    std::atomic<bool> stopping{false};

    Driver *driver = nullptr;
    TimerQueue timers;
    std::atomic<Worker *> timekeeper{nullptr};
};

} // namespace detail
} // namespace sched
} // namespace stackfull
