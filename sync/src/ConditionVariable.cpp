#include <stackfull/sync/ConditionVariable.h>

namespace stackfull {
namespace sync {

void ConditionVariable::wait(Mutex &mutex) {
    detail::Waiter waiter;
    {
        SpinLockGuard const guard(lock);
        waiters.pushBack(waiter);
    }
    // Registered before the mutex is released: a notifier that changes the
    // condition under the mutex cannot miss us.
    mutex.unlock();
    waiter.wait();
    mutex.lock();
}

void ConditionVariable::notifyOne() noexcept {
    detail::Waiter *waiter = nullptr;
    {
        SpinLockGuard const guard(lock);
        waiter = waiters.popFront();
    }
    if (waiter != nullptr) {
        waiter->notify();
    }
}

void ConditionVariable::notifyAll() noexcept {
    // Waiters live on their own stacks and may vanish right after notify();
    // detach the whole list under the lock, then notify from the detached
    // chain (re-using `next`, which unlink() cleared, as the chain link).
    detail::Waiter *chain = nullptr;
    {
        SpinLockGuard const guard(lock);
        while (detail::Waiter *const waiter = waiters.popFront()) {
            waiter->next = chain;
            chain = waiter;
        }
    }
    while (chain != nullptr) {
        detail::Waiter *const waiter = chain;
        chain = waiter->next; // read before notify(): the waiter may be gone afterwards
        waiter->notify();
    }
}

} // namespace sync
} // namespace stackfull
