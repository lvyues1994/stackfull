#pragma once

#include <stackfull/coro/detail/ContextBlock.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace stackfull {
namespace sched {
namespace detail {

struct Worker;
struct SchedulerCore;
struct JoinState;

// park()/wake() protocol state. Transitions:
//
//   Running  --park(): switch out, then onTaskParked-->  Parked
//   Parked   --wake()-->  Notified --> Running (+ scheduled)
//   Running  --wake()-->  Notified            (token kept for the next park)
//   Notified --onTaskParked-->  Running (+ scheduled immediately)
//   Running  --body returned-->  Done
//
// The Parked transition happens *after* the switch, on the arriving side,
// so a waker can never schedule a task whose registers are still being saved.
enum class TaskState : std::uint8_t { Running, Parked, Notified, Done };

// A scheduled coroutine. Lives at the top of its own stack like a plain
// ContextBlock; the extra fields are the scheduler's bookkeeping.
struct Task : coro::detail::ContextBlock {
    std::atomic<std::uint8_t> parkState{static_cast<std::uint8_t>(TaskState::Running)};

    // Link for a worker's pinned inbox (MpscQueue).
    std::atomic<Task *> mpscNext{nullptr};

    SchedulerCore *scheduler = nullptr;
    // Non-null while pinned: the task may only run on this worker.
    Worker *pinnedTo = nullptr;
    // Non-null for tasks created with spawnJoinable().
    JoinState *join = nullptr;

    // Wake-token slab coordinates.
    std::uint32_t slot = 0;
    std::uint32_t generation = 0;

    // Bottom of the stack when SchedulerOptions::checkStackCanary is set.
    std::uint64_t *stackCanary = nullptr;

    TaskState parkStateNow(std::memory_order const order = std::memory_order_acquire) const noexcept {
        return static_cast<TaskState>(parkState.load(order));
    }
};

inline std::uint8_t raw(TaskState const state) noexcept {
    return static_cast<std::uint8_t>(state);
}

// A cache line of this pattern sits at the bottom of each stack when
// SchedulerOptions::checkStackCanary is set.
constexpr std::uint64_t kStackCanary = 0x5AFE57AC6CA11A57ull;
constexpr std::size_t kStackCanaryWords = 8;

} // namespace detail
} // namespace sched
} // namespace stackfull
