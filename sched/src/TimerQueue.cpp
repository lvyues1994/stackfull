#include <stackfull/sched/detail/TimerQueue.h>

#include <atomic>
#include <cstddef>
#include <thread>
#include <utility>

namespace stackfull {
namespace sched {
namespace detail {

void TimerQueue::lock() noexcept {
    for (unsigned spins = 0;; ++spins) {
        if (not locked.load(std::memory_order_relaxed) and not locked.exchange(true, std::memory_order_acquire)) {
            return;
        }
        if (spins >= 32) {
            std::this_thread::yield();
        }
    }
}

bool TimerQueue::add(TimerEntry &entry) noexcept {
    lock();
    entry.fired.store(false, std::memory_order_relaxed);
    entry.heapIndex = heap.size();
    entry.queue = this;
    heap.push_back(&entry);
    siftUp(entry.heapIndex);
    bool const becameEarliest = heap[0] == &entry;
    publishLocked();
    unlock();
    return becameEarliest;
}

bool TimerQueue::cancel(TimerEntry &entry) noexcept {
    lock();
    std::size_t const index = entry.heapIndex;
    if (index == TimerEntry::kNotQueued) {
        unlock();
        return false;
    }
    std::size_t const last = heap.size() - 1;
    if (index != last) {
        swapAt(index, last);
    }
    heap.pop_back();
    entry.heapIndex = TimerEntry::kNotQueued;
    if (index != last) {
        siftDown(index);
        siftUp(index);
    }
    publishLocked();
    unlock();
    return true;
}

std::size_t TimerQueue::fireExpired(TimePoint const now) noexcept {
    std::size_t fired = 0;
    for (;;) {
        lock();
        if (heap.empty() or heap[0]->deadline > now) {
            unlock();
            return fired;
        }
        TimerEntry &entry = *heap[0];
        std::size_t const last = heap.size() - 1;
        if (last != 0) {
            swapAt(0, last);
        }
        heap.pop_back();
        entry.heapIndex = TimerEntry::kNotQueued;
        if (last != 0) {
            siftDown(0);
        }
        publishLocked();
        // Copy the token and mark fired *inside* the lock; wake outside it.
        // The sleeper may return (and free the entry) as soon as it sees
        // `fired`, so the entry is not touched after this point.
        WakeToken const token = entry.token;
        entry.fired.store(true, std::memory_order_release);
        unlock();
        token.wake();
        ++fired;
    }
}

// The active bit changes only on empty <-> non-empty and before `earliest`,
// so a reader that loads the mask and then `earliest` never skips a queue
// whose new deadline it would otherwise have seen.
void TimerQueue::publishLocked() noexcept {
    std::size_t const before = size.load(std::memory_order_relaxed);
    std::size_t const now = heap.size();
    if (activeMask != nullptr) {
        if (before == 0 and now != 0) {
            activeMask->fetch_or(activeBit, std::memory_order_seq_cst);
        } else if (before != 0 and now == 0) {
            activeMask->fetch_and(~activeBit, std::memory_order_seq_cst);
        }
    }
    size.store(now, std::memory_order_relaxed);
    earliest.store(heap.empty() ? kNoDeadline : ticksOf(heap[0]->deadline), std::memory_order_seq_cst);
}

void TimerQueue::swapAt(std::size_t const a, std::size_t const b) noexcept {
    std::swap(heap[a], heap[b]);
    heap[a]->heapIndex = a;
    heap[b]->heapIndex = b;
}

void TimerQueue::siftUp(std::size_t index) noexcept {
    while (index != 0) {
        std::size_t const parent = (index - 1) / 2;
        if (not(heap[index]->deadline < heap[parent]->deadline)) {
            return;
        }
        swapAt(index, parent);
        index = parent;
    }
}

void TimerQueue::siftDown(std::size_t index) noexcept {
    std::size_t const size = heap.size();
    for (;;) {
        std::size_t const left = 2 * index + 1;
        std::size_t const right = left + 1;
        std::size_t smallest = index;
        if (left < size and heap[left]->deadline < heap[smallest]->deadline) {
            smallest = left;
        }
        if (right < size and heap[right]->deadline < heap[smallest]->deadline) {
            smallest = right;
        }
        if (smallest == index) {
            return;
        }
        swapAt(index, smallest);
        index = smallest;
    }
}

} // namespace detail
} // namespace sched
} // namespace stackfull
