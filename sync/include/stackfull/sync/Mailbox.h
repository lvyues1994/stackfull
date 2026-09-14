#pragma once

#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

#include <chrono>
#include <cstddef>
#include <deque>
#include <functional>
#include <utility>

namespace stackfull {
namespace sync {

// Unbounded queue of closures from any thread to one task (an actor's
// inbox). The natural adapter for listener interfaces: each virtual
// function posts "what to do" and the owning task runs the closures in
// call order, in task context — where they may park, take a Mutex, do IO,
// none of which the SDK's thread could.
//
// Unbounded because dropping a control event (onStopped) is worse than
// growing; every post() already allocates for the std::function.
struct Mailbox {
    Mailbox() = default;
    Mailbox(Mailbox const &) = delete;
    Mailbox &operator=(Mailbox const &) = delete;

    // Any thread. Dropped once closed.
    template <class F>
    void post(F &&function) {
        std::function<void()> closure(std::forward<F>(function));
        SpinLockGuard const guard(lock);
        if (closed) {
            return;
        }
        queue.push_back(std::move(closure));
        consumer.wake();
    }

    // Wakes the consumer; runOne() returns false once the queue is drained.
    void close() noexcept {
        detail::Waker toWake;
        {
            SpinLockGuard const guard(lock);
            closed = true;
            toWake = consumer;
        }
        toWake.wake();
    }

    // Waits for one closure and runs it. False when closed and drained.
    bool runOne() {
        std::function<void()> closure;
        if (not next(closure)) {
            return false;
        }
        closure();
        return true;
    }

    // False on timeout; true when a closure ran. Closed-and-drained also
    // returns false — check isClosed() to tell them apart.
    template <class Rep, class Period>
    bool runOneFor(std::chrono::duration<Rep, Period> const timeout) {
        std::function<void()> closure;
        if (not nextUntil(std::chrono::steady_clock::now() +
                              std::chrono::duration_cast<std::chrono::nanoseconds>(timeout),
                          closure)) {
            return false;
        }
        closure();
        return true;
    }

    // Runs closures until close(); returns once the queue is drained.
    void run() {
        while (runOne()) {
        }
    }

    bool isClosed() const noexcept {
        SpinLockGuard const guard(lock);
        return closed;
    }

    std::size_t size() const noexcept {
        SpinLockGuard const guard(lock);
        return queue.size();
    }

    // --- select() support -----------------------------------------------------
    bool isReady() const noexcept {
        SpinLockGuard const guard(lock);
        return not queue.empty() or closed;
    }
    bool attach(detail::Waiter const &waiter) noexcept {
        SpinLockGuard const guard(lock);
        if (not queue.empty() or closed) {
            return false;
        }
        consumer = waiter.waker();
        return true;
    }
    void detach(detail::Waiter const &waiter) noexcept {
        SpinLockGuard const guard(lock);
        if (consumer == waiter.waker()) {
            consumer.clear();
        }
    }

private:
    struct DetachOnExit {
        Mailbox &mailbox;
        detail::Waiter &waiter;
        ~DetachOnExit() { mailbox.detach(waiter); }
    };

    bool next(std::function<void()> &out) {
        detail::Waiter waiter;
        DetachOnExit const detach{*this, waiter};
        for (;;) {
            {
                SpinLockGuard const guard(lock);
                if (not queue.empty()) {
                    out = std::move(queue.front());
                    queue.pop_front();
                    return true;
                }
                if (closed) {
                    return false;
                }
                consumer = waiter.waker();
            }
            waiter.block();
        }
    }

    bool nextUntil(std::chrono::steady_clock::time_point const deadline, std::function<void()> &out) {
        detail::Waiter waiter;
        DetachOnExit const detach{*this, waiter};
        for (;;) {
            {
                SpinLockGuard const guard(lock);
                if (not queue.empty()) {
                    out = std::move(queue.front());
                    queue.pop_front();
                    return true;
                }
                if (closed) {
                    return false;
                }
                consumer = waiter.waker();
            }
            if (not waiter.blockUntil(deadline)) {
                SpinLockGuard const guard(lock);
                if (queue.empty()) {
                    return false;
                }
                out = std::move(queue.front());
                queue.pop_front();
                return true;
            }
        }
    }

    mutable SpinLock lock;
    std::deque<std::function<void()>> queue;
    bool closed = false;
    detail::Waker consumer;
};

} // namespace sync
} // namespace stackfull
