#pragma once

#include <stackfull/sched/Parker.h>
#include <stackfull/sched/WakeToken.h>
#include <stackfull/sync/SpinLock.h>

#include <atomic>

namespace stackfull {
namespace sync {
namespace detail {

// One blocked party, living on the waiter's own stack (coroutine or OS
// thread) for as long as it is linked into a primitive's WaitList. Works for
// scheduled tasks (park/wake) and for plain threads (a thread-local Parker),
// so a primitive can be used from both sides — e.g. the main thread joining
// on a WaitGroup that tasks count down.
//
// Lifetime rule: the notifier may touch the Waiter only until it has called
// wake()/unpark(); right after that the waiter may return and the object dies.
// A waiter that leaves early (forced unwind at Scheduler::stop()) must first
// unlink itself under the primitive's lock — see WaitGuard.
struct Waiter {
    Waiter *next = nullptr;
    Waiter *prev = nullptr;
    // Maintained by WaitList under the owning primitive's lock.
    bool linked = false;
    std::atomic<bool> satisfied{false};

    // Exactly one of the two is set.
    sched::WakeToken token{};
    sched::Parker *parker = nullptr;

    // Binds to the calling context: the current task, or this OS thread.
    Waiter() noexcept;
    Waiter(Waiter const &) = delete;
    Waiter &operator=(Waiter const &) = delete;

    // Blocks the current task or thread until notify(). Not noexcept: a
    // forced unwind is thrown at the park inside.
    void wait();

    // Called by the notifier after unlinking the waiter.
    void notify() noexcept {
        if (parker != nullptr) {
            sched::Parker *const p = parker;
            satisfied.store(true, std::memory_order_release);
            p->unpark();
        } else {
            sched::WakeToken const t = token;
            satisfied.store(true, std::memory_order_release);
            t.wake();
        }
    }

    // A notifier that already unlinked us is about to write `satisfied`;
    // wait for that so the object may be destroyed safely.
    void awaitNotifier() noexcept;
};

// Intrusive FIFO of waiters. Not synchronized: callers hold the owning
// primitive's SpinLock.
struct WaitList {
    bool isEmpty() const noexcept { return head == nullptr; }

    void pushBack(Waiter &waiter) noexcept {
        waiter.next = nullptr;
        waiter.prev = tail;
        waiter.linked = true;
        if (tail != nullptr) {
            tail->next = &waiter;
        } else {
            head = &waiter;
        }
        tail = &waiter;
    }

    Waiter *popFront() noexcept {
        Waiter *const waiter = head;
        if (waiter != nullptr) {
            unlink(*waiter);
        }
        return waiter;
    }

    void unlink(Waiter &waiter) noexcept {
        if (waiter.prev != nullptr) {
            waiter.prev->next = waiter.next;
        } else {
            head = waiter.next;
        }
        if (waiter.next != nullptr) {
            waiter.next->prev = waiter.prev;
        } else {
            tail = waiter.prev;
        }
        waiter.next = nullptr;
        waiter.prev = nullptr;
        waiter.linked = false;
    }

private:
    Waiter *head = nullptr;
    Waiter *tail = nullptr;
};

// Scope guard for a wait on a WaitList. Armed by default; disarm() once the
// wait completed normally. If the scope is left otherwise (forced unwind)
// the destructor removes the waiter under the lock, or — when a notifier
// already took it — waits for that notifier to finish with the object.
// `onUnlinked` runs under the lock right after an unlink (e.g. to fix a
// waiter counter).
template <class OnUnlinked>
struct WaitGuard {
    WaitGuard(SpinLock &lock_, WaitList &list_, Waiter &waiter_, OnUnlinked onUnlinked_) noexcept
        : lock(lock_), list(list_), waiter(waiter_), onUnlinked(onUnlinked_) {}
    WaitGuard(WaitGuard const &) = delete;
    WaitGuard &operator=(WaitGuard const &) = delete;

    ~WaitGuard() {
        if (not armed) {
            return;
        }
        bool wasLinked = false;
        {
            SpinLockGuard const guard(lock);
            wasLinked = waiter.linked;
            if (wasLinked) {
                list.unlink(waiter);
                onUnlinked();
            }
        }
        if (not wasLinked) {
            waiter.awaitNotifier();
        }
    }

    void disarm() noexcept { armed = false; }

private:
    SpinLock &lock;
    WaitList &list;
    Waiter &waiter;
    OnUnlinked onUnlinked;
    bool armed = true;
};

// Usage (C++14 has no guaranteed elision, so construct in place):
//   auto fixCount = [&] { waiterCount.fetch_sub(1); };
//   detail::WaitGuard<decltype(fixCount)> guard(lock, waiters, waiter, fixCount);
//   waiter.wait();
//   guard.disarm();
struct NoOp {
    void operator()() const noexcept {}
};

} // namespace detail
} // namespace sync
} // namespace stackfull
