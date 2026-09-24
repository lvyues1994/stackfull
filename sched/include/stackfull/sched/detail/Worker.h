#pragma once

#include <stackfull/coro/detail/ContextBlock.h>
#include <stackfull/coro/detail/ThreadState.h>
#include <stackfull/sched/Parker.h>
#include <stackfull/sched/Stats.h>
#include <stackfull/sched/detail/MpscQueue.h>
#include <stackfull/sched/detail/SchedulerCore.h>
#include <stackfull/sched/detail/Task.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace stackfull {
namespace sched {
namespace detail {

// One OS thread of the scheduler. Owns a local run queue, a LIFO slot for
// the most recently woken task, an inbox for tasks pinned to it, and the
// parker it sleeps on. The dispatcher context is the thread's native stack.
//
// Hot members (local queue, LIFO slot) are touched only by the owning thread
// except for thieves reading the queue; `pinnedInbox` and `parker` are
// touched by other threads and sit on their own cache lines.
struct Worker {
    Worker(SchedulerCore &core_, std::size_t index_);
    Worker(Worker const &) = delete;
    Worker &operator=(Worker const &) = delete;

    // C++14 `new` only guarantees alignof(max_align_t); the queue members
    // are alignas(64), so Worker provides its own aligned allocation.
    static void *operator new(std::size_t size);
    static void operator delete(void *memory) noexcept;

    // --- hot, inline (Runtime.h) ------------------------------------------
    // LIFO slot, then local queue, then pinned inbox.
    Task *takeLocal() noexcept;
    // Next runnable task on this worker or, if none, the dispatcher.
    coro::detail::ContextBlock &nextOrDispatcher() noexcept;
    // Tail of the local queue; overflows into the injection queue.
    void pushLocalFifo(Task &task) noexcept;
    // LIFO slot (evicting its occupant to the queue tail), capped so a
    // ping-pong pair cannot starve the queue.
    void pushLocalLifo(Task &task) noexcept;
    bool hasLocalWork() const noexcept;
    // Appends to the batch being gathered (see `gathering`).
    void gather(Task &task) noexcept;

    // --- cold (Worker.cpp) ------------------------------------------------
    // Dispatcher loop; returns when the scheduler has fully stopped.
    void run();
    Task *steal() noexcept;
    Task *searchForWork() noexcept;
    void wakePeerForLeftovers() noexcept;
    // Bounded busy-wait as a searcher before sleeping.
    Task *spinForWork() noexcept;
    // Sleeps until notified; returns work found before or instead of sleeping.
    Task *parkIdle() noexcept;
    void sleepIdle() noexcept;
    void maintain() noexcept;
    void reapParkedTasks() noexcept;
    // Places the gathered batch. With `keepFirst` the first task is returned
    // for this worker to run; the rest go to the injection queue behind a
    // single wakeup (or stay local when no peer is idle).
    Task *placeGathered(bool keepFirst) noexcept;

    SchedulerCore &core;
    std::size_t const index;

    // Valid while run() executes.
    coro::ThreadState *thread = nullptr;
    coro::detail::ContextBlock *dispatcher = nullptr;

    alignas(64) LocalQueue local;
    Task *lifoSlot = nullptr;
    unsigned lifoStreak = 0;
    unsigned tick = 0;
    std::atomic<unsigned> yieldTick{0}; // also read by the stall monitor
    std::uint32_t rng = 0;
    bool isSearching = false;
    // Exponential backoff of idle spinning (see spinForWork).
    unsigned spinMisses = 0;
    unsigned spinSkips = 0;
    // Bumped whenever a task parks or finishes; the stall monitor watches it
    // together with yieldTick.
    std::atomic<std::uint32_t> switchCount{0};

    // While set, schedule() on this thread gathers the tasks it wakes into a
    // batch (linked through Task::mpscNext) instead of placing each one:
    // firing timers or polling the driver, a burst of wakeups then costs one
    // peer wakeup rather than one per task.
    bool gathering = false;
    Task *gatheredHead = nullptr;
    Task *gatheredTail = nullptr;

    alignas(64) MpscQueue<Task> pinnedInbox;
    alignas(64) std::unique_ptr<Parker> parker;
    std::atomic<bool> running{false};
    // Set by a peer worker that unparks us to hand over a task: we are in a
    // worker-to-worker exchange, where spinning pays off even if it did not
    // while the exchange ran through futex wakes. Foreign threads leave it.
    std::atomic<bool> handoffWake{false};

    // Free wake-token slots kept by this worker so that spawning and
    // finishing tasks do not all contend on the global free list. The lock
    // is uncontended except when the global list runs dry and another
    // thread takes slots from here (see SchedulerCore::acquireSlot).
    struct SlotCache {
        static constexpr std::uint32_t kCapacity = 64;
        static constexpr std::uint32_t kBatch = 32; // moved to/from the global list at once
        std::atomic<bool> locked{false};
        std::uint32_t count = 0;
        std::uint32_t slots[kCapacity];
    };
    alignas(64) SlotCache slotCache;

    // Tasks this worker created and finished. Single writer; summed by
    // SchedulerCore::liveTasks().
    std::atomic<std::uint64_t> tasksCreated{0};
    std::atomic<std::uint64_t> tasksFinished{0};
    // SchedulerStats counters, single writer.
    std::atomic<std::uint64_t> sleeps{0};
    std::atomic<std::uint64_t> steals{0};
    std::atomic<std::uint64_t> wakeLatency[kWakeLatencyBuckets] = {};

    // Deadlines added by tasks while running here; fired by any worker.
    alignas(64) TimerQueue timers;
};

// Post-switch hooks (Runtime.h). Each runs on the arriving side once the
// departing task's registers are saved; `arg` is the Worker the switch
// happened on.
void onTaskYielded(coro::detail::ContextBlock &suspended, void *arg) noexcept;
void onTaskParked(coro::detail::ContextBlock &suspended, void *arg) noexcept;
void onTaskFinished(coro::detail::ContextBlock &finished, void *arg) noexcept;

// First-entry trampoline for tasks (Task.cpp).
void taskEntry(fcontext::transfer_t transfer);

} // namespace detail
} // namespace sched
} // namespace stackfull
