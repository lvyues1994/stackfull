#include <stackfull/sync/Semaphore.h>

#include <atomic>
#include <cstddef>

namespace stackfull {
namespace sync {

// Lost-wakeup argument (Dekker / store-buffering shape). An acquirer that
// finds no permit *first* announces itself (waiterCount++) and *then*
// re-checks `permits`; a releaser first adds its permit and then reads
// `waiterCount`. All four accesses are seq_cst, so at least one side sees
// the other's write: either the acquirer sees the permit, or the releaser
// sees the waiter and hands the permit over under the lock. Announcing
// after the check (check-then-store on both sides) is not Dekker and loses
// wakeups — found by the ConditionVariable producer/consumer test.

bool Semaphore::tryAcquire() noexcept {
    std::size_t count = permits.load(std::memory_order_seq_cst);
    while (count > 0) {
        if (permits.compare_exchange_weak(count, count - 1, std::memory_order_seq_cst)) {
            return true;
        }
    }
    return false;
}

void Semaphore::acquire() {
    if (tryAcquire()) {
        return;
    }
    acquireSlow();
}

void Semaphore::acquireSlow() {
    detail::Waiter waiter;
    {
        SpinLockGuard const guard(lock);
        waiterCount.fetch_add(1, std::memory_order_seq_cst); // announce first ...
        if (tryAcquire()) {                                   // ... then re-check
            waiterCount.fetch_sub(1, std::memory_order_seq_cst);
            return;
        }
        waiters.pushBack(waiter);
    }
    // Leaving through a forced unwind: drop out of the list (and the count).
    auto const fixCount = [this] { waiterCount.fetch_sub(1, std::memory_order_seq_cst); };
    detail::WaitGuard<decltype(fixCount)> unlinkOnUnwind(lock, waiters, waiter, fixCount);
    waiter.wait(); // a permit was taken on our behalf by release()
    unlinkOnUnwind.disarm();
}

detail::EnqueueResult Semaphore::acquireOrEnqueue(detail::Waiter &waiter) noexcept {
    if (tryAcquire()) {
        return detail::EnqueueResult::Ready;
    }
    SpinLockGuard const guard(lock);
    if (waiter.cancelled) {
        return detail::EnqueueResult::Cancelled;
    }
    waiterCount.fetch_add(1, std::memory_order_seq_cst);
    if (tryAcquire()) {
        waiterCount.fetch_sub(1, std::memory_order_seq_cst);
        return detail::EnqueueResult::Ready;
    }
    waiters.pushBack(waiter);
    return detail::EnqueueResult::Enqueued;
}

bool Semaphore::cancel(detail::Waiter &waiter) noexcept {
    SpinLockGuard const guard(lock);
    if (not detail::cancelWaiter(waiters, waiter)) {
        return false;
    }
    waiterCount.fetch_sub(1, std::memory_order_seq_cst);
    return true;
}

void Semaphore::release() noexcept {
    permits.fetch_add(1, std::memory_order_seq_cst);
    if (waiterCount.load(std::memory_order_seq_cst) == 0) {
        return;
    }
    // Hand out every permit a waiter can have, in FIFO order, and notify
    // once the lock is released: a callback waiter's continuation runs inside
    // notify() and may well come back to this semaphore.
    detail::Waiter *first = nullptr;
    detail::Waiter **last = &first;
    {
        SpinLockGuard const guard(lock);
        while (not waiters.isEmpty()) {
            std::size_t count = permits.load(std::memory_order_seq_cst);
            bool taken = false;
            while (count > 0 and not taken) {
                taken = permits.compare_exchange_weak(count, count - 1, std::memory_order_seq_cst);
            }
            if (not taken) {
                break; // a fast-path acquirer got there first; the rest keep waiting
            }
            detail::Waiter *const waiter = waiters.popFront();
            waiterCount.fetch_sub(1, std::memory_order_seq_cst);
            *last = waiter;
            last = &waiter->next;
        }
    }
    *last = nullptr;
    while (first != nullptr) {
        detail::Waiter *const waiter = first;
        first = waiter->next; // read before notify(): the waiter may be gone right after
        waiter->notify();
    }
}

} // namespace sync
} // namespace stackfull
