#pragma once

#include <stackfull/queue/detail/PackedCursor.h>

#include <atomic>
#include <cstddef>
#include <type_traits>

namespace stackfull {
namespace queue {

// Go-runtime style local run queue: a fixed ring with an owner-only tail and
// a shared head that both the owner (pop) and thieves (steal) CAS.
//
// Same static interface as BwosQueue so the scheduler can be instantiated
// with either. This is the *reference*: simple enough to be obviously
// correct, used as the differential-test oracle and the benchmark baseline
// that BwosQueue has to beat. Every owner pop pays one CAS on a cache line
// thieves also write — the cost BWoS exists to remove.
template <class T, std::size_t Capacity>
struct RingQueue {
    static_assert(std::is_trivially_copyable<T>::value, "RingQueue stores trivially copyable values");
    static_assert(detail::isPowerOfTwo(Capacity) and Capacity >= 2, "Capacity must be a power of two >= 2");

    static constexpr std::size_t kCapacity = Capacity;
    // Largest batch a single steal/popBlock may move: half the ring.
    static constexpr std::size_t kEntriesPerBlock = Capacity / 2;

    RingQueue() noexcept = default;
    RingQueue(RingQueue const &) = delete;
    RingQueue &operator=(RingQueue const &) = delete;

    // ---------------------------------------------------------------------
    // Owner side
    // ---------------------------------------------------------------------

    bool push(T const value) noexcept {
        std::size_t const t = tail.load(std::memory_order_relaxed);
        std::size_t const h = head.load(std::memory_order_acquire);
        if (t - h >= Capacity) {
            return false;
        }
        buffer[t & kMask].store(value, std::memory_order_relaxed);
        tail.store(t + 1, std::memory_order_release); // publishes the slot to thieves
        return true;
    }

    template <class It>
    std::size_t pushBatch(It first, It const last) noexcept {
        std::size_t pushed = 0;
        while (first != last and push(*first)) {
            ++first;
            ++pushed;
        }
        return pushed;
    }

    bool pop(T &out) noexcept {
        for (;;) {
            std::size_t h = head.load(std::memory_order_acquire);
            std::size_t const t = tail.load(std::memory_order_relaxed);
            if (h == t) {
                return false;
            }
            out = buffer[h & kMask].load(std::memory_order_relaxed);
            if (head.compare_exchange_weak(h, h + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
        }
    }

    // Owner-side bulk removal of half the queue (for overflow).
    std::size_t popBlock(T *const out) noexcept { return grab(out, /*half=*/true); }

    std::size_t minRemainingSlots() const noexcept {
        std::size_t const t = tail.load(std::memory_order_relaxed);
        std::size_t const h = head.load(std::memory_order_acquire);
        return Capacity - (t - h);
    }

    bool hasStealable() const noexcept { return hasEntries(); }

    bool hasEntries() const noexcept {
        return tail.load(std::memory_order_acquire) != head.load(std::memory_order_acquire);
    }

    bool producerBlockHasActiveStealers() const noexcept { return false; }

    // ---------------------------------------------------------------------
    // Thief side
    // ---------------------------------------------------------------------

    bool steal(T &out) noexcept {
        for (;;) {
            std::size_t h = head.load(std::memory_order_acquire);
            std::size_t const t = tail.load(std::memory_order_acquire);
            if (h == t) {
                return false;
            }
            // Copy before the CAS: the owner cannot overwrite slot h until
            // head has moved past it, which would make our CAS fail. A read
            // that loses the race is discarded; the slots are atomics so that
            // discarded read is not a data race in the C++ model either.
            out = buffer[h & kMask].load(std::memory_order_relaxed);
            if (head.compare_exchange_weak(h, h + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
        }
    }

    // Steals half of the visible entries (Go's runqgrab).
    std::size_t stealBlock(T *const out) noexcept { return grab(out, /*half=*/true); }

    bool isEmptyForThief() const noexcept { return not hasEntries(); }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    std::size_t grab(T *const out, bool const half) noexcept {
        for (;;) {
            std::size_t h = head.load(std::memory_order_acquire);
            std::size_t const t = tail.load(std::memory_order_acquire);
            std::size_t const available = t - h;
            std::size_t n = half ? available - available / 2 : available;
            if (n == 0) {
                return 0;
            }
            if (n > kEntriesPerBlock) {
                n = kEntriesPerBlock;
            }
            for (std::size_t i = 0; i < n; ++i) {
                out[i] = buffer[(h + i) & kMask].load(std::memory_order_relaxed);
            }
            if (head.compare_exchange_weak(h, h + n, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return n;
            }
        }
    }

    alignas(detail::kCacheLineSize) std::atomic<std::size_t> head{0};
    alignas(detail::kCacheLineSize) std::atomic<std::size_t> tail{0};
    alignas(detail::kCacheLineSize) std::atomic<T> buffer[Capacity];
};

template <class T, std::size_t Capacity>
constexpr std::size_t RingQueue<T, Capacity>::kCapacity;
template <class T, std::size_t Capacity>
constexpr std::size_t RingQueue<T, Capacity>::kEntriesPerBlock;

} // namespace queue
} // namespace stackfull
