#pragma once

#include <atomic>
#include <thread>

namespace stackfull {
namespace sync {

// Guards the few-instruction critical sections inside the primitives
// (waiter-list manipulation). Test-and-test-and-set with a bounded spin,
// then yields the OS thread so a preempted holder can finish.
//
// Never held across a park() — that would block every other worker.
struct SpinLock {
    SpinLock() noexcept = default;
    SpinLock(SpinLock const &) = delete;
    SpinLock &operator=(SpinLock const &) = delete;

    void lock() noexcept {
        for (unsigned spins = 0;; ++spins) {
            if (not locked.load(std::memory_order_relaxed) and
                not locked.exchange(true, std::memory_order_acquire)) {
                return;
            }
            if (spins >= kSpinsBeforeYield) {
                std::this_thread::yield();
            }
        }
    }

    bool tryLock() noexcept {
        return not locked.load(std::memory_order_relaxed) and not locked.exchange(true, std::memory_order_acquire);
    }

    void unlock() noexcept { locked.store(false, std::memory_order_release); }

private:
    static constexpr unsigned kSpinsBeforeYield = 32;
    std::atomic<bool> locked{false};
};

struct SpinLockGuard {
    explicit SpinLockGuard(SpinLock &lock_) noexcept : lock(lock_) { lock.lock(); }
    ~SpinLockGuard() { lock.unlock(); }
    SpinLockGuard(SpinLockGuard const &) = delete;
    SpinLockGuard &operator=(SpinLockGuard const &) = delete;

private:
    SpinLock &lock;
};

} // namespace sync
} // namespace stackfull
