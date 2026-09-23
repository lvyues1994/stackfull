#pragma once

#include <stackfull/sched/WakeToken.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace stackfull {
namespace sched {
namespace detail {

using TimePoint = std::chrono::steady_clock::time_point;

// Deadlines as plain integers, for the lock-free mirrors below.
constexpr std::int64_t kNoDeadline = std::numeric_limits<std::int64_t>::max();
inline std::int64_t ticksOf(TimePoint const time) noexcept {
    return static_cast<std::int64_t>(time.time_since_epoch().count());
}

struct TimerQueue;

// One pending deadline. Lives on the sleeping task's stack for as long as it
// is queued; the queue drops its reference under the lock before the token
// is woken, so the owner may return as soon as it observes `fired`.
struct TimerEntry {
    TimePoint deadline{};
    WakeToken token{};
    TimerQueue *queue = nullptr; // where add() put it: cancel there, wherever the task runs now
    std::size_t heapIndex = kNotQueued;
    std::atomic<bool> fired{false};

    static constexpr std::size_t kNotQueued = static_cast<std::size_t>(-1);
};

// Binary min-heap of intrusive entries behind a small lock, one per worker:
// a task adds to its current worker's queue, so adds do not contend across
// workers. Entries are pointers into the sleepers' stacks; nothing is
// allocated per timer. Size and earliest deadline are mirrored in atomics
// so that others can look without taking the lock.
struct TimerQueue {
    TimerQueue() = default;
    TimerQueue(TimerQueue const &) = delete;
    TimerQueue &operator=(TimerQueue const &) = delete;

    // `bit` is set in `mask` while this queue holds entries, so that others
    // look only at non-empty queues. Before any add().
    void attachActiveMask(std::atomic<std::uint64_t> &mask, std::uint64_t bit) noexcept {
        activeMask = &mask;
        activeBit = bit;
    }

    // True if `entry` became the earliest deadline (the timekeeper must be
    // told to shorten its sleep).
    bool add(TimerEntry &entry) noexcept;

    // True if the entry was still queued (and is now removed); false if it
    // already fired.
    bool cancel(TimerEntry &entry) noexcept;

    // Wakes every entry with deadline <= now. Returns how many.
    std::size_t fireExpired(TimePoint now) noexcept;

    // Lock-free and possibly stale by one operation: scheduling heuristics only.
    bool isEmptyApprox() const noexcept { return size.load(std::memory_order_relaxed) == 0; }

    // Earliest pending deadline or kNoDeadline. seq_cst, updated under the
    // lock after every change: the timekeeper protocol relies on it (see
    // SchedulerCore::addTimer).
    std::int64_t earliestTicks() const noexcept { return earliest.load(std::memory_order_seq_cst); }

private:
    void siftUp(std::size_t index) noexcept;
    void siftDown(std::size_t index) noexcept;
    void swapAt(std::size_t a, std::size_t b) noexcept;
    void publishLocked() noexcept;

    std::atomic<bool> locked{false};
    void lock() noexcept;
    void unlock() noexcept { locked.store(false, std::memory_order_release); }

    std::vector<TimerEntry *> heap;
    std::atomic<std::size_t> size{0};
    std::atomic<std::int64_t> earliest{kNoDeadline};
    std::atomic<std::uint64_t> *activeMask = nullptr;
    std::uint64_t activeBit = 0;
};

// Removes a still-queued entry from whichever queue holds it; false if it
// already fired (or was never added).
inline bool cancelTimer(TimerEntry &entry) noexcept {
    return entry.queue != nullptr and entry.queue->cancel(entry);
}

} // namespace detail
} // namespace sched
} // namespace stackfull
