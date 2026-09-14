#pragma once

#include <stackfull/sched/Parker.h>
#include <stackfull/sched/WakeToken.h>

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
struct Waiter {
    Waiter *next = nullptr;
    Waiter *prev = nullptr;
    std::atomic<bool> satisfied{false};

    // Exactly one of the two is set.
    sched::WakeToken token{};
    sched::Parker *parker = nullptr;

    // Binds to the calling context: the current task, or this OS thread.
    Waiter() noexcept;
    Waiter(Waiter const &) = delete;
    Waiter &operator=(Waiter const &) = delete;

    // Blocks the current task or thread until notify().
    void wait() noexcept;

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
};

// Intrusive FIFO of waiters. Not synchronized: callers hold the owning
// primitive's SpinLock.
struct WaitList {
    bool isEmpty() const noexcept { return head == nullptr; }

    void pushBack(Waiter &waiter) noexcept {
        waiter.next = nullptr;
        waiter.prev = tail;
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
    }

private:
    Waiter *head = nullptr;
    Waiter *tail = nullptr;
};

} // namespace detail
} // namespace sync
} // namespace stackfull
