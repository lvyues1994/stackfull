#pragma once

#include <stackfull/queue/detail/PackedCursor.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace stackfull {
namespace queue {

enum class PushStatus : std::uint8_t {
    Ok,
    Full, // every block holds unconsumed data
    Busy  // a dequeue or another enqueue is mid-flight in the block we need; retry
};

enum class PopStatus : std::uint8_t {
    Ok,
    Empty,
    Busy // an allocated entry has not been committed yet; retry
};

// Block-based Bounded Queue (Wang et al., ATC '22), retry-new mode.
// Lock-free bounded MPMC FIFO — see PROVENANCE.md.
//
// The ring of NumBlocks blocks carries two queue-level heads (producer /
// consumer) and four block-level cursors:
//
//   allocated  slots handed to producers (FAA)      committed  slots written
//   reserved   slots handed to consumers (CAS)      consumed   slots read
//
// Producers and consumers in different blocks touch disjoint cache lines;
// only moving to the next block (rare) uses queue-level CAS. Like BWoS this
// costs one block of capacity: at most (NumBlocks - 1) * BlockSize items.
//
// Both operations are total and never spin internally. `Busy` is a
// transient state the caller is expected to retry, with back-off of its
// choice; in the scheduler it only arises while another thread is between
// its allocate and commit, a window of a few instructions.
template <class T, std::size_t NumBlocks, std::size_t BlockSize>
struct BbqQueue {
    static_assert(std::is_trivially_copyable<T>::value, "BbqQueue stores trivially copyable values");
    static_assert(detail::isPowerOfTwo(NumBlocks) and NumBlocks >= 2, "NumBlocks must be a power of two >= 2");
    static_assert(BlockSize >= 1, "BlockSize must be positive");

    static constexpr std::size_t kNumBlocks = NumBlocks;
    static constexpr std::size_t kBlockSize = BlockSize;
    static constexpr std::size_t kCapacity = (NumBlocks - 1) * BlockSize;

    BbqQueue() noexcept {
        for (std::size_t i = 0; i < NumBlocks; ++i) {
            // First block: empty, round 0. Others: "fully consumed in round 0"
            // so the producer may advance into them.
            Cursor const initial = Cursor::make(0, i == 0 ? 0 : BlockSize);
            blocks[i].allocated.store(initial.raw, std::memory_order_relaxed);
            blocks[i].committed.store(initial.raw, std::memory_order_relaxed);
            blocks[i].reserved.store(initial.raw, std::memory_order_relaxed);
            blocks[i].consumed.store(initial.raw, std::memory_order_relaxed);
        }
        producerHead.store(0, std::memory_order_relaxed);
        consumerHead.store(0, std::memory_order_relaxed);
    }

    BbqQueue(BbqQueue const &) = delete;
    BbqQueue &operator=(BbqQueue const &) = delete;

    PushStatus push(T const value) noexcept {
        for (;;) {
            Head const head{producerHead.load(std::memory_order_acquire)};
            Block &block = blocks[head.index()];

            // --- allocate_entry ---------------------------------------------
            // Pre-check keeps `allocated` from running away through FAA on a
            // full block; the FAA result is still authoritative.
            Cursor const peek{block.allocated.load(std::memory_order_relaxed)};
            if (peek.index() < BlockSize) {
                Cursor const slot{block.allocated.fetch_add(1, std::memory_order_relaxed)};
                if (slot.index() < BlockSize) {
                    block.entries[slot.index()] = value;
                    // Pairs with the acquire load of `committed` in pop().
                    block.committed.fetch_add(1, std::memory_order_release);
                    return PushStatus::Ok;
                }
            }

            // --- advance_phead ----------------------------------------------
            Block &next = blocks[(head.index() + 1) & (NumBlocks - 1)];
            Cursor const consumed{next.consumed.load(std::memory_order_acquire)};
            bool const previousRoundLeft =
                consumed.version() == head.version() and consumed.index() == BlockSize;
            bool const laterRound = consumed.version() > head.version();
            if (not previousRoundLeft and not laterRound) {
                Cursor const reserved{next.reserved.load(std::memory_order_acquire)};
                // Unconsumed data with no consumer inside → genuinely full;
                // a consumer mid-read → transient.
                return reserved.index() == consumed.index() ? PushStatus::Full : PushStatus::Busy;
            }
            // (The paper's extra `committed` check here belongs to drop-old
            // mode; in retry-new "fully consumed" already implies it.)
            Cursor const fresh = Cursor::make(head.version() + 1, 0);
            atomicMax(next.committed, fresh.raw, std::memory_order_release);
            atomicMax(next.allocated, fresh.raw, std::memory_order_release);
            atomicMax(producerHead, head.raw + 1, std::memory_order_release);
        }
    }

    PopStatus pop(T &out) noexcept {
        for (;;) {
            Head const head{consumerHead.load(std::memory_order_acquire)};
            Block &block = blocks[head.index()];

            // --- reserve_entry ----------------------------------------------
            Cursor reserved{block.reserved.load(std::memory_order_acquire)};
            while (reserved.index() < BlockSize) {
                Cursor const committed{block.committed.load(std::memory_order_acquire)};
                if (reserved.index() == committed.index()) {
                    return PopStatus::Empty;
                }
                if (committed.index() != BlockSize) {
                    // Slots are committed out of order; only when every
                    // allocated slot is committed is *our* slot guaranteed.
                    Cursor const allocated{block.allocated.load(std::memory_order_acquire)};
                    if (allocated.index() != committed.index()) {
                        return PopStatus::Busy;
                    }
                }
                std::size_t expected = reserved.raw;
                if (block.reserved.compare_exchange_weak(expected, reserved.addIndex(1).raw,
                                                         std::memory_order_acq_rel, std::memory_order_acquire)) {
                    out = block.entries[reserved.index()];
                    // Pairs with the acquire of `consumed` in push(): the
                    // producer may only recycle the block once every read is done.
                    block.consumed.fetch_add(1, std::memory_order_release);
                    return PopStatus::Ok;
                }
                reserved = Cursor{expected}; // lost the race; retry with the fresh value
            }

            // --- advance_chead ----------------------------------------------
            Block &next = blocks[(head.index() + 1) & (NumBlocks - 1)];
            Cursor const committed{next.committed.load(std::memory_order_acquire)};
            if (committed.version() != head.version() + 1) {
                return PopStatus::Empty; // the producer has not entered it yet
            }
            Cursor const fresh = Cursor::make(head.version() + 1, 0);
            atomicMax(next.consumed, fresh.raw, std::memory_order_release);
            atomicMax(next.reserved, fresh.raw, std::memory_order_release);
            atomicMax(consumerHead, head.raw + 1, std::memory_order_release);
        }
    }

private:
    // Head: version | block index. The index occupies exactly log2(NumBlocks)
    // bits so `head + 1` carries into the version when the ring wraps.
    using Head = detail::PackedCursor<detail::bitsToHold(NumBlocks - 1)>;

    // Cursor: version | slot offset. The offset field is wider than needed
    // for BlockSize so concurrent FAAs past the end are detected, not wrapped.
    static constexpr unsigned kOverflowBits = 10;
    using Cursor = detail::PackedCursor<detail::bitsToHold(BlockSize) + kOverflowBits>;

    struct Block {
        alignas(detail::kCacheLineSize) std::atomic<std::size_t> allocated{0};
        alignas(detail::kCacheLineSize) std::atomic<std::size_t> committed{0};
        alignas(detail::kCacheLineSize) std::atomic<std::size_t> reserved{0};
        alignas(detail::kCacheLineSize) std::atomic<std::size_t> consumed{0};
        alignas(detail::kCacheLineSize) T entries[BlockSize];
    };

    // Monotonic update: the paper's MAX instruction, as a CAS loop. Versions
    // live in the high bits, so a plain integer compare orders (version, index).
    static void atomicMax(std::atomic<std::size_t> &target, std::size_t const value,
                          std::memory_order const order) noexcept {
        std::size_t current = target.load(std::memory_order_relaxed);
        while (current < value and
               not target.compare_exchange_weak(current, value, order, std::memory_order_relaxed)) {
        }
    }

    alignas(detail::kCacheLineSize) std::atomic<std::size_t> producerHead{0};
    alignas(detail::kCacheLineSize) std::atomic<std::size_t> consumerHead{0};
    alignas(detail::kCacheLineSize) Block blocks[NumBlocks];
};

template <class T, std::size_t NumBlocks, std::size_t BlockSize>
constexpr std::size_t BbqQueue<T, NumBlocks, BlockSize>::kNumBlocks;
template <class T, std::size_t NumBlocks, std::size_t BlockSize>
constexpr std::size_t BbqQueue<T, NumBlocks, BlockSize>::kBlockSize;
template <class T, std::size_t NumBlocks, std::size_t BlockSize>
constexpr std::size_t BbqQueue<T, NumBlocks, BlockSize>::kCapacity;

} // namespace queue
} // namespace stackfull
