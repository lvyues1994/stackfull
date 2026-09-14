#pragma once

#include <stackfull/sync/Mutex.h>
#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

namespace stackfull {
namespace sync {

// Condition variable over sync::Mutex with the usual contract: change the
// condition while holding the mutex, notify after; wait() may return
// spuriously, so always re-check the predicate.
struct ConditionVariable {
    ConditionVariable() noexcept = default;
    ConditionVariable(ConditionVariable const &) = delete;
    ConditionVariable &operator=(ConditionVariable const &) = delete;

    // Precondition: `mutex` is held by the caller; it is released while
    // waiting and re-acquired before returning.
    void wait(Mutex &mutex);

    template <class Predicate>
    void wait(Mutex &mutex, Predicate predicate) {
        while (not predicate()) {
            wait(mutex);
        }
    }

    void notifyOne() noexcept;
    void notifyAll() noexcept;

private:
    SpinLock lock;
    detail::WaitList waiters;
};

} // namespace sync
} // namespace stackfull
