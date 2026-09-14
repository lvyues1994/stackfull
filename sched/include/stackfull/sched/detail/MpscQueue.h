#pragma once

#include <atomic>

namespace stackfull {
namespace sched {
namespace detail {

// Intrusive, unbounded, lock-free-push / single-consumer queue (Vyukov).
// Nodes carry their own `std::atomic<Node *> mpscNext`; nothing is allocated.
//
// Used for a worker's *pinned inbox*: tasks bound to one worker that any
// thread may wake. Push is wait-free; pop is single-consumer and may report
// "inconsistent" for a moment while a producer is between its two stores —
// the consumer simply tries again later.
template <class Node>
struct MpscQueue {
    MpscQueue() noexcept {
        stub.mpscNext.store(nullptr, std::memory_order_relaxed);
        head.store(&stub, std::memory_order_relaxed);
        tail = &stub;
    }
    MpscQueue(MpscQueue const &) = delete;
    MpscQueue &operator=(MpscQueue const &) = delete;

    // Any thread.
    void push(Node &node) noexcept {
        node.mpscNext.store(nullptr, std::memory_order_relaxed);
        Node *const previous = head.exchange(&node, std::memory_order_acq_rel);
        previous->mpscNext.store(&node, std::memory_order_release);
    }

    // Consumer only. nullptr when empty or momentarily inconsistent.
    Node *pop() noexcept {
        Node *front = tail;
        Node *next = front->mpscNext.load(std::memory_order_acquire);
        if (front == &stub) {
            if (next == nullptr) {
                return nullptr;
            }
            tail = next;
            front = next;
            next = next->mpscNext.load(std::memory_order_acquire);
        }
        if (next != nullptr) {
            tail = next;
            return front;
        }
        if (front != head.load(std::memory_order_acquire)) {
            return nullptr; // a producer is mid-push; come back later
        }
        push(stub);
        next = front->mpscNext.load(std::memory_order_acquire);
        if (next != nullptr) {
            tail = next;
            return front;
        }
        return nullptr;
    }

    // Consumer only; approximate (may be stale by one producer step).
    bool isEmpty() const noexcept {
        return tail == &stub and stub.mpscNext.load(std::memory_order_acquire) == nullptr;
    }

private:
    Node stub;
    std::atomic<Node *> head;
    Node *tail;
};

} // namespace detail
} // namespace sched
} // namespace stackfull
