#pragma once

#include <stackfull/coro/Fatal.h>
#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace stackfull {
namespace sync {

// Bounded MPMC channel (capacity >= 1). send() parks while full, recv()
// parks while empty; close() wakes everyone: further sends fail, receives
// drain what is buffered and then fail.
//
// The ring and both waiter lists sit behind one SpinLock; the critical
// sections are a handful of instructions and no switch happens inside them.
template <class T>
struct Channel {
    explicit Channel(std::size_t const capacity_)
        : capacity(capacity_), slots(std::make_unique<Slot[]>(capacity_)) {
        STACKFULL_CHECK(capacity_ >= 1, "stackfull: Channel capacity must be at least 1");
    }
    Channel(Channel const &) = delete;
    Channel &operator=(Channel const &) = delete;
    ~Channel() {
        while (count != 0) {
            slotAt(head)->~T();
            head = (head + 1) % capacity;
            --count;
        }
    }

    // False once closed.
    bool send(T value) {
        for (;;) {
            lock.lock();
            if (closed) {
                lock.unlock();
                return false;
            }
            if (count < capacity) {
                pushLocked(std::move(value));
                detail::Waiter *const receiver = receivers.popFront();
                lock.unlock();
                if (receiver != nullptr) {
                    receiver->notify();
                }
                return true;
            }
            detail::Waiter waiter;
            senders.pushBack(waiter);
            lock.unlock();
            waiter.wait();
        }
    }

    bool trySend(T value) {
        SpinLockGuard const guard(lock);
        if (closed or count == capacity) {
            return false;
        }
        pushLocked(std::move(value));
        if (detail::Waiter *const receiver = receivers.popFront()) {
            receiver->notify();
        }
        return true;
    }

    // False when closed and drained.
    bool recv(T &out) {
        for (;;) {
            lock.lock();
            if (count != 0) {
                out = popLocked();
                detail::Waiter *const sender = senders.popFront();
                lock.unlock();
                if (sender != nullptr) {
                    sender->notify();
                }
                return true;
            }
            if (closed) {
                lock.unlock();
                return false;
            }
            detail::Waiter waiter;
            receivers.pushBack(waiter);
            lock.unlock();
            waiter.wait();
        }
    }

    bool tryRecv(T &out) {
        SpinLockGuard const guard(lock);
        if (count == 0) {
            return false;
        }
        out = popLocked();
        if (detail::Waiter *const sender = senders.popFront()) {
            sender->notify();
        }
        return true;
    }

    void close() noexcept {
        detail::Waiter *chain = nullptr;
        {
            SpinLockGuard const guard(lock);
            closed = true;
            while (detail::Waiter *const waiter = senders.popFront()) {
                waiter->next = chain;
                chain = waiter;
            }
            while (detail::Waiter *const waiter = receivers.popFront()) {
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

    bool isClosed() const noexcept {
        SpinLockGuard const guard(lock);
        return closed;
    }

    std::size_t size() const noexcept {
        SpinLockGuard const guard(lock);
        return count;
    }

private:
    using Slot = typename std::aligned_storage<sizeof(T), alignof(T)>::type;

    T *slotAt(std::size_t const index) noexcept { return reinterpret_cast<T *>(&slots[index]); }

    void pushLocked(T &&value) {
        ::new (static_cast<void *>(&slots[(head + count) % capacity])) T(std::move(value));
        ++count;
    }

    T popLocked() {
        T *const slot = slotAt(head);
        T value(std::move(*slot));
        slot->~T();
        head = (head + 1) % capacity;
        --count;
        return value;
    }

    std::size_t const capacity;
    std::unique_ptr<Slot[]> slots;
    std::size_t head = 0;
    std::size_t count = 0;
    bool closed = false;
    mutable SpinLock lock;
    detail::WaitList senders;
    detail::WaitList receivers;
};

} // namespace sync
} // namespace stackfull
