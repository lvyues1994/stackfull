#pragma once

#include <stackfull/sched/WakeToken.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <vector>

namespace stackfull {
namespace sched {
namespace detail {

using TimePoint = std::chrono::steady_clock::time_point;

// One pending deadline. Lives on the sleeping task's stack (or the waiting
// thread's) for as long as it is queued; the queue drops its reference under
// the lock before the token is woken, so the owner may return as soon as it
// observes `fired`.
struct TimerEntry {
    TimePoint deadline{};
    WakeToken token{};
    std::size_t heapIndex = kNotQueued;
    std::atomic<bool> fired{false};

    static constexpr std::size_t kNotQueued = static_cast<std::size_t>(-1);
};

// Global binary min-heap of intrusive entries behind a small lock. Timer
// traffic (sleep, timed wait) is orders of magnitude rarer than switches, so
// one lock for the whole scheduler is fine; entries are pointers into the
// sleepers' stacks, so nothing is allocated per timer.
struct TimerQueue {
    TimerQueue() = default;
    TimerQueue(TimerQueue const &) = delete;
    TimerQueue &operator=(TimerQueue const &) = delete;

    // True if `entry` became the earliest deadline (the timekeeper must be
    // told to shorten its sleep).
    bool add(TimerEntry &entry) noexcept;

    // True if the entry was still queued (and is now removed); false if it
    // already fired.
    bool cancel(TimerEntry &entry) noexcept;

    // Wakes every entry with deadline <= now. Returns how many.
    std::size_t fireExpired(TimePoint now) noexcept;

    // Earliest pending deadline, if any.
    bool nextDeadline(TimePoint &out) noexcept;

    // Lock-free and possibly stale by one operation: scheduling heuristics only.
    bool isEmptyApprox() const noexcept { return size.load(std::memory_order_relaxed) == 0; }

private:
    void siftUp(std::size_t index) noexcept;
    void siftDown(std::size_t index) noexcept;
    void swapAt(std::size_t a, std::size_t b) noexcept;

    std::atomic<bool> locked{false};
    void lock() noexcept;
    void unlock() noexcept { locked.store(false, std::memory_order_release); }

    std::vector<TimerEntry *> heap;
    std::atomic<std::size_t> size{0}; // heap.size(), mirrored for isEmptyApprox()
};

} // namespace detail
} // namespace sched
} // namespace stackfull
