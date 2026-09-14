#include <stackfull/sync/WaitGroup.h>

#include <stackfull/coro/Fatal.h>

namespace stackfull {
namespace sync {

void WaitGroup::done() noexcept {
    std::size_t const before = pending.fetch_sub(1, std::memory_order_acq_rel);
    STACKFULL_CHECK(before != 0, "stackfull: WaitGroup::done() without matching add()");
    if (before != 1) {
        return;
    }
    // Reached zero: release everyone. Same detach-then-notify pattern as
    // ConditionVariable::notifyAll.
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
        chain = waiter->next;
        waiter->notify();
    }
}

void WaitGroup::wait() {
    if (pending.load(std::memory_order_acquire) == 0) {
        return;
    }
    detail::Waiter waiter;
    {
        SpinLockGuard const guard(lock);
        // Re-check under the lock: done() takes the lock after its decrement,
        // so a zero we miss here is followed by a pop that finds us.
        if (pending.load(std::memory_order_acquire) == 0) {
            return;
        }
        waiters.pushBack(waiter);
    }
    detail::WaitGuard<detail::NoOp> unlinkOnUnwind(lock, waiters, waiter, detail::NoOp{});
    waiter.wait();
    unlinkOnUnwind.disarm();
}

} // namespace sync
} // namespace stackfull
