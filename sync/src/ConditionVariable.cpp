#include <stackfull/sync/ConditionVariable.h>

namespace stackfull {
namespace sync {

namespace {

// Forced unwind out of wait(): the caller's guard will unlock the mutex, so
// try to hold it again. Parking here is not an option (the unwinder would
// find us Parked a second time), so this is best effort: if another dying
// task holds the mutex we give up and the mutex ends in an inconsistent
// state — acceptable only because it happens at Scheduler::stop().
struct RelockOnUnwind {
    Mutex &mutex;
    bool armed = true;
    ~RelockOnUnwind() {
        if (armed) {
            for (unsigned spins = 0; spins < 1024 and not mutex.tryLock(); ++spins) {
            }
        }
    }
};

} // namespace

void ConditionVariable::wait(Mutex &mutex) {
    detail::Waiter waiter;
    {
        SpinLockGuard const guard(lock);
        waiters.pushBack(waiter);
    }
    // Registered before the mutex is released: a notifier that changes the
    // condition under the mutex cannot miss us.
    mutex.unlock();
    {
        RelockOnUnwind relock{mutex};
        detail::WaitGuard<detail::NoOp> unlinkOnUnwind(lock, waiters, waiter, detail::NoOp{});
        waiter.wait();
        unlinkOnUnwind.disarm();
        relock.armed = false;
    }
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
