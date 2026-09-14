#pragma once

#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

#include <atomic>
#include <cstddef>

namespace stackfull {
namespace sync {

// Counting semaphore usable from tasks (park) and from OS threads (block).
//
// Uncontended acquire/release are one CAS / one fetch_add. Blocked acquirers
// wait FIFO on an intrusive list; release() hands surplus permits to them
// directly. Fast-path acquirers may still take a permit ahead of a woken
// waiter (barging), which favours throughput over strict fairness.
struct Semaphore {
    explicit Semaphore(std::size_t const initialPermits) noexcept : permits(initialPermits) {}
    Semaphore(Semaphore const &) = delete;
    Semaphore &operator=(Semaphore const &) = delete;

    void acquire();
    bool tryAcquire() noexcept;
    void release() noexcept;

    std::size_t available() const noexcept { return permits.load(std::memory_order_acquire); }

private:
    void acquireSlow();

    std::atomic<std::size_t> permits;
    std::atomic<std::size_t> waiterCount{0};
    SpinLock lock;
    detail::WaitList waiters;
};

} // namespace sync
} // namespace stackfull
