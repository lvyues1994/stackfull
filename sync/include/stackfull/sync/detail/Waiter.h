#pragma once

#include <stackfull/sched/Parker.h>
#include <stackfull/sched/WakeToken.h>
#include <stackfull/sync/SpinLock.h>

#include <atomic>
#include <chrono>
#include <cstdint>

namespace stackfull {
namespace sync {
namespace detail {

// The third way to wait, beside a task and a thread: a function to call
// (how stackfull::exec's operations wait). It runs on the notifying thread
// once the notifier has released its locks, and may complete — and so
// destroy — whatever owns the waiter. A callback waiter is woken once:
// sources take it when they wake it. Must not throw; not spelled noexcept
// because that changes the type's mangling between C++14 and C++17.
using WakeCallback = void (*)(void *argument);

// Value handle for waking a blocked party: a task's WakeToken, a plain
// thread's Parker, or a callback. Safe to keep after a task or thread moved
// on — waking a task that no longer waits (or no longer exists) is a no-op
// or a spurious wake, which every wait loop tolerates. Event sources that
// outlive their awaiters (Completion, Stream, Mailbox) store this instead of
// a pointer to a stack-resident Waiter, so even a task released without
// unwinding (no-exceptions builds) leaves nothing dangling behind. The one
// thing a stale token still needs is the Scheduler it belongs to: keep
// schedulers alive for as long as callbacks may arrive (the default one is
// immortal).
struct Waker {
    sched::WakeToken token{};
    sched::Parker *parker = nullptr;
    WakeCallback callback = nullptr;
    void *argument = nullptr;

    void wake() const noexcept {
        if (callback != nullptr) {
            callback(argument);
        } else if (parker != nullptr) {
            parker->unpark();
        } else if (token) {
            token.wake();
        }
    }
    bool isEmpty() const noexcept { return callback == nullptr and parker == nullptr and not token; }
    void clear() noexcept { *this = Waker{}; }
    friend bool operator==(Waker const &a, Waker const &b) noexcept {
        return a.callback == b.callback and a.argument == b.argument and a.parker == b.parker and
               a.token.core == b.token.core and a.token.slot == b.token.slot and a.token.generation == b.token.generation;
    }
};

// Queueing a callback waiter with a primitive: no need to wait (try the
// operation now), queued (the callback comes), or the primitive's cancel()
// came first (complete as stopped).
enum class EnqueueResult : std::uint8_t { Ready, Enqueued, Cancelled };

// The same for operations that move a value (Channel, Stream): it moved,
// the source is closed, it would wait but no waiter was given (an attempt
// only), queued, or cancelled first.
enum class TransferResult : std::uint8_t { Done, Closed, WouldBlock, Enqueued, Cancelled };

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

    // Exactly one of the three is set.
    sched::WakeToken token{};
    sched::Parker *parker = nullptr;
    WakeCallback callback = nullptr;
    void *argument = nullptr;
    // Callback waiters: a cancel() found the waiter not queued, so the next
    // enqueue refuses it. Read and written under the primitive's lock.
    bool cancelled = false;

    // Binds to the calling context: the current task, or this OS thread.
    Waiter() noexcept;
    // A callback waiter (see WakeCallback); wait() and block() do not apply.
    Waiter(WakeCallback const callback_, void *const argument_) noexcept : callback(callback_), argument(argument_) {}
    Waiter(Waiter const &) = delete;
    Waiter &operator=(Waiter const &) = delete;

    // Blocks the current task or thread until notify(). Not noexcept: a
    // forced unwind is thrown at the park inside.
    void wait();

    // Condition-less variants for sources that keep their own readiness
    // state and store a Waker by value: block once (spurious returns
    // allowed), or block until `deadline` (false on timeout). A task uses
    // the scheduler's timer queue, a thread its Parker's timed park.
    void block();
    bool blockUntil(std::chrono::steady_clock::time_point deadline);

    Waker waker() const noexcept { return Waker{token, parker, callback, argument}; }

    // Called by the notifier after unlinking the waiter, outside its locks.
    void notify() noexcept {
        if (callback != nullptr) {
            WakeCallback const f = callback;
            f(argument); // may destroy this waiter
            return;
        }
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

// cancel() for a callback waiter on a WaitList, under the primitive's lock:
// true if it was queued and now is not (the caller completes it as
// stopped); otherwise it is marked so that its next enqueue is refused —
// or it was already taken by a notifier, whose callback is on its way.
inline bool cancelWaiter(WaitList &list, Waiter &waiter) noexcept {
    if (waiter.linked) {
        list.unlink(waiter);
        return true;
    }
    waiter.cancelled = true;
    return false;
}

} // namespace detail
} // namespace sync
} // namespace stackfull
