#pragma once

#include <stackfull/coro/detail/ContextBlock.h>
#include <stackfull/coro/detail/ThreadState.h>
#include <stackfull/sched/Parker.h>
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

    // --- cold (Worker.cpp) ------------------------------------------------
    // Dispatcher loop; returns when the scheduler has fully stopped.
    void run();
    Task *steal() noexcept;
    Task *searchForWork() noexcept;
    // Bounded busy-wait as a searcher before sleeping.
    Task *spinForWork() noexcept;
    // Sleeps until notified; returns work found before or instead of sleeping.
    Task *parkIdle() noexcept;
    void reapParkedTasks() noexcept;

    SchedulerCore &core;
    std::size_t const index;

    // Valid while run() executes.
    coro::ThreadState *thread = nullptr;
    coro::detail::ContextBlock *dispatcher = nullptr;

    alignas(64) LocalQueue local;
    Task *lifoSlot = nullptr;
    unsigned lifoStreak = 0;
    unsigned tick = 0;
    unsigned yieldTick = 0;
    std::uint32_t rng = 0;
    bool isSearching = false;

    alignas(64) MpscQueue<Task> pinnedInbox;
    alignas(64) std::unique_ptr<Parker> parker;
    std::atomic<bool> running{false};
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
