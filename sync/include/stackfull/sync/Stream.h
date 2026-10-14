#pragma once

#include <stackfull/coro/Fatal.h>
#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <system_error>
#include <type_traits>
#include <utility>

namespace stackfull {
namespace sync {

enum class Overflow : std::uint8_t {
    DropOldest, // real-time streams: the consumer sees the freshest items
    DropNewest, // logs / commands: what was accepted stays
};

// Bounded stream from producers that must never block (SDK callback
// threads) to one consumer that waits. push() from any thread is a lock,
// a copy into the ring and a wake; when the ring is full an item is dropped
// according to the policy — DropOldest by default.
//
// next() parks a task or blocks a plain thread; nextFor() adds a deadline.
// close() ends the stream: next() drains what is buffered, then returns
// false. Single consumer.
template <class T>
struct Stream {
    explicit Stream(std::size_t const capacity_, Overflow const policy_ = Overflow::DropOldest)
        : capacity(capacity_), policy(policy_), slots(std::make_unique<Slot[]>(capacity_)) {
        STACKFULL_CHECK(capacity_ >= 1, "stackfull: Stream capacity must be at least 1");
    }
    Stream(Stream const &) = delete;
    Stream &operator=(Stream const &) = delete;
    ~Stream() {
        while (count != 0) {
            slotAt(head)->~T();
            head = (head + 1) % capacity;
            --count;
        }
    }

    // --- producers (any thread) ---------------------------------------------

    // Never blocks. Dropped silently after close() or per the policy.
    void push(T value) {
        lock.lock();
        if (closed or (count == capacity and policy == Overflow::DropNewest)) {
            lock.unlock();
            droppedCount.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        if (count == capacity) {
            slotAt(head)->~T(); // DropOldest
            head = (head + 1) % capacity;
            --count;
            droppedCount.fetch_add(1, std::memory_order_relaxed);
        }
        ::new (static_cast<void *>(&slots[(head + count) % capacity])) T(std::move(value));
        ++count;
        detail::Waker const toWake = takeConsumerLocked();
        lock.unlock();
        toWake.wake(); // a Waker by value: harmless if the consumer moved on
    }

    void close() noexcept {
        lock.lock();
        closed = true;
        detail::Waker const toWake = takeConsumerLocked();
        lock.unlock();
        toWake.wake();
    }

    // --- consumer -----------------------------------------------------------

    bool tryNext(T &out) {
        SpinLockGuard const guard(lock);
        if (count == 0) {
            return false;
        }
        out = popLocked();
        return true;
    }

    // False once closed and drained.
    bool next(T &out) {
        detail::Waiter waiter;
        DetachOnExit const detach{*this, waiter};
        for (;;) {
            {
                SpinLockGuard const guard(lock);
                if (count != 0) {
                    out = popLocked();
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

    // False on timeout, or once closed and drained (see isClosed()).
    bool nextUntil(std::chrono::steady_clock::time_point const deadline, T &out) {
        detail::Waiter waiter;
        DetachOnExit const detach{*this, waiter};
        for (;;) {
            {
                SpinLockGuard const guard(lock);
                if (count != 0) {
                    out = popLocked();
                    return true;
                }
                if (closed) {
                    return false;
                }
                consumer = waiter.waker();
            }
            if (not waiter.blockUntil(deadline)) {
                return tryNext(out); // the item may have arrived as we timed out
            }
        }
    }

    template <class Rep, class Period>
    bool nextFor(std::chrono::duration<Rep, Period> const timeout, T &out) {
        return nextUntil(std::chrono::steady_clock::now() +
                             std::chrono::duration_cast<std::chrono::nanoseconds>(timeout),
                         out);
    }

    bool isClosed() const noexcept {
        SpinLockGuard const guard(lock);
        return closed;
    }

    std::size_t size() const noexcept {
        SpinLockGuard const guard(lock);
        return count;
    }

    // Items discarded so far (overflow or after close). Diagnostics.
    std::size_t dropped() const noexcept { return droppedCount.load(std::memory_order_relaxed); }

    // --- select() support -----------------------------------------------------
    bool isReady() const noexcept {
        SpinLockGuard const guard(lock);
        return count != 0 or closed;
    }
    bool attach(detail::Waiter const &waiter) noexcept {
        SpinLockGuard const guard(lock);
        if (count != 0 or closed) {
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

    // next() for a callback waiter (stackfull::exec); a null waiter only
    // tries. Hands the item to `sink(T &&)`, called under the lock, when
    // Done; Closed once closed and drained; after an Enqueued the callback
    // means: try again.
    template <class Sink>
    detail::TransferResult nextOrAttach(Sink &&sink, detail::Waiter *const waiter) {
        SpinLockGuard const guard(lock);
        if (count != 0) {
            sink(popLocked());
            return detail::TransferResult::Done;
        }
        if (closed) {
            return detail::TransferResult::Closed;
        }
        if (waiter == nullptr) {
            return detail::TransferResult::WouldBlock;
        }
        if (waiter->cancelled) {
            return detail::TransferResult::Cancelled;
        }
        consumer = waiter->waker();
        return detail::TransferResult::Enqueued;
    }
    // True if `waiter` was attached and now is not.
    bool cancel(detail::Waiter &waiter) noexcept {
        SpinLockGuard const guard(lock);
        if (consumer == waiter.waker()) {
            consumer.clear();
            return true;
        }
        waiter.cancelled = true;
        return false;
    }

private:
    using Slot = typename std::aligned_storage<sizeof(T), alignof(T)>::type;

    // A wake is one-shot: a callback waiter must not be called twice.
    detail::Waker takeConsumerLocked() noexcept {
        detail::Waker const taken = consumer;
        consumer.clear();
        return taken;
    }

    struct DetachOnExit {
        Stream &stream;
        detail::Waiter &waiter;
        ~DetachOnExit() { stream.detach(waiter); }
    };

    T *slotAt(std::size_t const index) noexcept { return reinterpret_cast<T *>(&slots[index]); }

    T popLocked() {
        T *const slot = slotAt(head);
        T value(std::move(*slot));
        slot->~T();
        head = (head + 1) % capacity;
        --count;
        return value;
    }

    std::size_t const capacity;
    Overflow const policy;
    std::unique_ptr<Slot[]> slots;
    std::size_t head = 0;
    std::size_t count = 0;
    bool closed = false;
    mutable SpinLock lock;
    detail::Waker consumer;
    std::atomic<std::size_t> droppedCount{0};
};

// A one-slot stream that always holds the most recent item: camera
// preview frames, sensor readings, "current state" updates.
template <class T>
struct Latest final : Stream<T> {
    Latest() : Stream<T>(1, Overflow::DropOldest) {}
};

} // namespace sync
} // namespace stackfull
