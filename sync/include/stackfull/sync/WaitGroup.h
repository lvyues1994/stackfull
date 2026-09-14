#pragma once

#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

#include <atomic>
#include <cstddef>

namespace stackfull {
namespace sync {

// Go-style WaitGroup: add() before spawning, done() in each task, wait()
// until the counter drops to zero. wait() parks a task or blocks a thread —
// the natural way for the main thread to wait for a batch of detached tasks.
struct WaitGroup {
    WaitGroup() noexcept = default;
    WaitGroup(WaitGroup const &) = delete;
    WaitGroup &operator=(WaitGroup const &) = delete;

    void add(std::size_t const count = 1) noexcept { pending.fetch_add(count, std::memory_order_acq_rel); }
    void done() noexcept;
    void wait();

    std::size_t pendingCount() const noexcept { return pending.load(std::memory_order_acquire); }

private:
    std::atomic<std::size_t> pending{0};
    SpinLock lock;
    detail::WaitList waiters;
};

} // namespace sync
} // namespace stackfull
