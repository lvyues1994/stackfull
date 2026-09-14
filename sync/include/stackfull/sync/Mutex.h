#pragma once

#include <stackfull/sync/Semaphore.h>

namespace stackfull {
namespace sync {

// Mutual exclusion for tasks: lock() parks the task instead of blocking the
// worker thread, so it is safe to hold across yield()/park(). Also usable
// from plain threads. Not recursive; unlock() by any context is allowed
// (it is a binary semaphore).
struct Mutex {
    Mutex() noexcept : permit(1) {}
    Mutex(Mutex const &) = delete;
    Mutex &operator=(Mutex const &) = delete;

    void lock() { permit.acquire(); }
    bool tryLock() noexcept { return permit.tryAcquire(); }
    void unlock() noexcept { permit.release(); }

private:
    Semaphore permit;
};

struct LockGuard {
    explicit LockGuard(Mutex &mutex_) : mutex(mutex_) { mutex.lock(); }
    ~LockGuard() { mutex.unlock(); }
    LockGuard(LockGuard const &) = delete;
    LockGuard &operator=(LockGuard const &) = delete;

private:
    Mutex &mutex;
};

} // namespace sync
} // namespace stackfull
