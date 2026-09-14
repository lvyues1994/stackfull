#pragma once

#include <stackfull/queue/detail/PackedCursor.h>

#include <atomic>
#include <cstddef>
#include <type_traits>

namespace stackfull {
namespace queue {

// Block-based Work Stealing queue (Wang et al., OSDI '23), ported from the
// Tokio reference implementation — see PROVENANCE.md.
//
// One *owner* thread pushes and pops; any number of *thief* threads steal.
// Storage is a ring of NumBlocks blocks, each with its own four cursors:
//
//   committed  producer progress        (owner writes, thieves read)
//   consumed   consumer progress        (owner only)
//   reserved   thief claim position     (thieves CAS; owner takes over)
//   stolen     thief completion count   (thieves add; owner reads on reuse)
//
// The owner's fast path touches only `committed` / `consumed` of its current
// block with relaxed atomics — no barrier, no CAS. Thieves may never operate
// on the block the consumer is in, and the producer may not enter a block
// until the previous round's consumer and thieves have left it. Both rules
// cost capacity: the queue holds at most (NumBlocks - 1) * EntriesPerBlock
// items, and items in the consumer's current block cannot be stolen.
//
// T must be trivially copyable (the scheduler stores Task pointers).
template <class T, std::size_t NumBlocks, std::size_t EntriesPerBlock>
struct BwosQueue {
    static_assert(std::is_trivially_copyable<T>::value, "BwosQueue stores trivially copyable values");
    static_assert(detail::isPowerOfTwo(NumBlocks) and NumBlocks >= 2, "NumBlocks must be a power of two >= 2");
    static_assert(EntriesPerBlock >= 1, "EntriesPerBlock must be positive");

    static constexpr std::size_t kNumBlocks = NumBlocks;
    static constexpr std::size_t kEntriesPerBlock = EntriesPerBlock;
    // Upper bound on simultaneously held items (see class comment).
    static constexpr std::size_t kCapacity = (NumBlocks - 1) * EntriesPerBlock;

    BwosQueue() noexcept {
        for (std::size_t i = 0; i < NumBlocks; ++i) {
            Block &block = blocks[i];
            bool const isHead = i == 0;
            block.isHead = isHead;
            block.next = &blocks[(i + 1) % NumBlocks];
            // The head block starts empty in round 1; the others start "fully
            // consumed in round 0" so the producer may enter them.
            Cursor const owner = isHead ? Cursor::make(1, 0) : Cursor::make(0, EntriesPerBlock);
            // Thieves start with every block fully reserved: the head is the
            // consumer's, the rest must first be entered by the producer.
            Cursor const thief = Cursor::make(isHead ? 1 : 0, EntriesPerBlock);
            block.committed.store(owner.raw, std::memory_order_relaxed);
            block.consumed.store(owner.raw, std::memory_order_relaxed);
            block.reserved.store(thief.raw, std::memory_order_relaxed);
            block.stolen.store(thief.raw, std::memory_order_relaxed);
        }
        producerBlock = &blocks[0];
        consumerBlock = &blocks[0];
        stealPosition.store(0, std::memory_order_relaxed);
    }

    BwosQueue(BwosQueue const &) = delete;
    BwosQueue &operator=(BwosQueue const &) = delete;

    // ---------------------------------------------------------------------
    // Owner side — single thread
    // ---------------------------------------------------------------------

    // False when the queue is full; the caller then overflows elsewhere.
    bool push(T const value) noexcept {
        for (;;) {
            Block &block = *producerBlock;
            Cursor const committed{block.committed.load(std::memory_order_relaxed)};
            std::size_t const index = committed.index();
            if (index < EntriesPerBlock) {
                block.entries[index] = value;
                // Pairs with the acquire load in steal()/stealBlock().
                block.committed.store(committed.addIndex(1).raw, std::memory_order_release);
                return true;
            }
            if (not advanceProducer(block, committed)) {
                return false;
            }
        }
    }

    // Enqueue as much of [first, last) as fits; returns the number pushed.
    // A caller that checked minRemainingSlots() beforehand gets all of them.
    template <class It>
    std::size_t pushBatch(It first, It const last) noexcept {
        std::size_t pushed = 0;
        while (first != last) {
            Block &block = *producerBlock;
            Cursor const committed{block.committed.load(std::memory_order_relaxed)};
            std::size_t index = committed.index();
            std::size_t count = 0;
            while (index < EntriesPerBlock and first != last) {
                block.entries[index] = *first;
                ++first;
                ++index;
                ++count;
            }
            if (count != 0) {
                block.committed.store(committed.addIndex(count).raw, std::memory_order_release);
                pushed += count;
            }
            if (first != last and not advanceProducer(block, committed.addIndex(count))) {
                break;
            }
        }
        return pushed;
    }

    bool pop(T &out) noexcept {
        Block *block = nullptr;
        Cursor consumed{};
        if (not locateConsumer(block, consumed)) {
            return false;
        }
        out = block->entries[consumed.index()];
        block->consumed.store(consumed.addIndex(1).raw, std::memory_order_relaxed);
        return true;
    }

    // Removes every committed entry of the consumer's current block into
    // `out` (capacity >= kEntriesPerBlock). Used when the queue overflows and
    // a whole block is handed to the injection queue.
    std::size_t popBlock(T *const out) noexcept {
        Block *block = nullptr;
        Cursor consumed{};
        if (not locateConsumer(block, consumed)) {
            return 0;
        }
        Cursor const committed{block->committed.load(std::memory_order_relaxed)};
        // Claim before copying: only this thread can push into the block.
        block->consumed.store(committed.raw, std::memory_order_relaxed);
        std::size_t const begin = consumed.index();
        std::size_t const end = committed.index();
        for (std::size_t i = begin; i < end; ++i) {
            out[i - begin] = block->entries[i];
        }
        return end - begin;
    }

    // Free slots guaranteed available to push(): current block plus the next
    // one if it is already writable. Enough for a steal of at most one block.
    std::size_t minRemainingSlots() const noexcept {
        Block const &block = *producerBlock;
        Cursor const committed{block.committed.load(std::memory_order_relaxed)};
        std::size_t free = EntriesPerBlock - committed.index();
        Block const &next = *block.next;
        Cursor const nextCommitted{next.committed.load(std::memory_order_relaxed)};
        if (isNextBlockWritable(next, nextCommitted.version())) {
            free += EntriesPerBlock;
        }
        return free;
    }

    // True if a thief could currently find something (may still fail).
    bool hasStealable() const noexcept {
        if (producerBlock == consumerBlock) {
            return false;
        }
        Block const &block = *producerBlock;
        Cursor const committed{block.committed.load(std::memory_order_relaxed)};
        Cursor const reserved{block.reserved.load(std::memory_order_acquire)};
        return reserved != committed;
    }

    // True if pop() would succeed right now. Exact for the owner in the
    // absence of thieves; with thieves a concurrent steal may still empty the
    // block the consumer is about to advance into (false positive only —
    // never a false negative, which would let a worker park on a non-empty
    // queue). Non-mutating: blocks ahead of the consumer are inspected
    // through the cursors the producer and thieves maintain, since their
    // `consumed` cursor is stale until the consumer takes them over.
    bool hasEntries() const noexcept {
        Block const *block = consumerBlock;
        Cursor consumed{block->consumed.load(std::memory_order_relaxed)};
        for (std::size_t i = 0; i < NumBlocks + 1; ++i) {
            if (consumed.index() < EntriesPerBlock) {
                Cursor const committed{block->committed.load(std::memory_order_relaxed)};
                return consumed.index() != committed.index();
            }
            Block const &next = *block->next;
            if (not consumerMayAdvance(next, consumed)) {
                return false;
            }
            std::size_t const nextVersion = consumed.version() + (next.isHead ? 1 : 0);
            Cursor const reserved{next.reserved.load(std::memory_order_acquire)};
            Cursor const committed{next.committed.load(std::memory_order_relaxed)};
            if (reserved.index() < committed.index()) {
                return true; // committed entries no thief has claimed
            }
            if (reserved.index() < EntriesPerBlock) {
                return false; // everything committed so far is claimed
            }
            consumed = Cursor::make(nextVersion, EntriesPerBlock); // fully claimed: keep walking
            block = &next;
        }
        return false;
    }

    // Thieves still copying out of the producer's current block. Consulted on
    // overflow: draining a block to the injection queue while thieves work on
    // it would only add contention.
    bool producerBlockHasActiveStealers() const noexcept {
        Block const &block = *producerBlock;
        Cursor const reserved{block.reserved.load(std::memory_order_relaxed)};
        Cursor const stolen{block.stolen.load(std::memory_order_relaxed)};
        return reserved != stolen;
    }

    // ---------------------------------------------------------------------
    // Thief side — any thread
    // ---------------------------------------------------------------------

    bool steal(T &out) noexcept {
        for (;;) {
            std::size_t const position = stealPosition.load(std::memory_order_relaxed);
            Block &block = blocks[position & (NumBlocks - 1)];

            Cursor const reserved{block.reserved.load(std::memory_order_acquire)};
            std::size_t const index = reserved.index();
            if (index < EntriesPerBlock) {
                Cursor const committed{block.committed.load(std::memory_order_acquire)};
                if (index == committed.index()) {
                    return false; // nothing committed beyond the claims
                }
                std::size_t expected = reserved.raw;
                if (not block.reserved.compare_exchange_weak(expected, reserved.addIndex(1).raw,
                                                             std::memory_order_release,
                                                             std::memory_order_relaxed)) {
                    continue; // another thief or the owner's takeover moved it
                }
                out = block.entries[index];
                // Pairs with the acquire in isNextBlockWritable(): the owner
                // may only recycle the block once every claim is copied out.
                block.stolen.fetch_add(1, std::memory_order_release);
                return true;
            }

            if (not thiefMayAdvance(block, reserved)) {
                return false;
            }
            std::size_t expectedPosition = position;
            stealPosition.compare_exchange_weak(expectedPosition, position + 1, std::memory_order_relaxed,
                                                std::memory_order_relaxed);
        }
    }

    // Claims every committed-but-unclaimed entry of the current thief block
    // into `out` (capacity >= kEntriesPerBlock). Returns the count.
    std::size_t stealBlock(T *const out) noexcept {
        for (;;) {
            std::size_t const position = stealPosition.load(std::memory_order_relaxed);
            Block &block = blocks[position & (NumBlocks - 1)];

            Cursor const reserved{block.reserved.load(std::memory_order_acquire)};
            std::size_t const begin = reserved.index();
            if (begin < EntriesPerBlock) {
                Cursor const committed{block.committed.load(std::memory_order_acquire)};
                std::size_t const end = committed.index();
                if (begin == end) {
                    return 0;
                }
                std::size_t expected = reserved.raw;
                if (not block.reserved.compare_exchange_weak(expected, committed.raw, std::memory_order_release,
                                                             std::memory_order_relaxed)) {
                    continue;
                }
                for (std::size_t i = begin; i < end; ++i) {
                    out[i - begin] = block.entries[i];
                }
                block.stolen.fetch_add(end - begin, std::memory_order_release);
                return end - begin;
            }

            if (not thiefMayAdvance(block, reserved)) {
                return 0;
            }
            std::size_t expectedPosition = position;
            stealPosition.compare_exchange_weak(expectedPosition, position + 1, std::memory_order_relaxed,
                                                std::memory_order_relaxed);
        }
    }

    // Expensive full scan from the thief position; meant for a parked owner
    // whose queue another worker wants to check for leftover work.
    bool isEmptyForThief() const noexcept {
        std::size_t const position = stealPosition.load(std::memory_order_acquire);
        for (std::size_t i = 0; i < NumBlocks + 1; ++i) {
            Block const &block = blocks[(position + i) & (NumBlocks - 1)];
            Cursor const reserved{block.reserved.load(std::memory_order_acquire)};
            if (reserved.index() < EntriesPerBlock) {
                Cursor const committed{block.committed.load(std::memory_order_acquire)};
                return reserved.index() == committed.index();
            }
            if (not thiefMayAdvance(block, reserved)) {
                return true;
            }
        }
        return true;
    }

private:
    using Cursor = detail::PackedCursor<detail::bitsToHold(EntriesPerBlock)>;

    struct Block {
        alignas(detail::kCacheLineSize) std::atomic<std::size_t> committed{0};
        alignas(detail::kCacheLineSize) std::atomic<std::size_t> consumed{0};
        alignas(detail::kCacheLineSize) std::atomic<std::size_t> reserved{0};
        alignas(detail::kCacheLineSize) std::atomic<std::size_t> stolen{0};
        alignas(detail::kCacheLineSize) Block *next = nullptr;
        bool isHead = false;
        alignas(detail::kCacheLineSize) T entries[EntriesPerBlock];
    };

    // --- producer -----------------------------------------------------------

    // The next block is free for round `nextVersion` once the previous
    // round's consumer *and* thieves have completely left it.
    static bool isNextBlockWritable(Block const &next, std::size_t const nextVersion) noexcept {
        std::size_t const previousVersion = nextVersion - 1;
        Cursor const consumed{next.consumed.load(std::memory_order_relaxed)};
        if (consumed.index() != EntriesPerBlock or consumed.version() != previousVersion) {
            return false;
        }
        Cursor const stolen{next.stolen.load(std::memory_order_acquire)};
        return stolen.index() == EntriesPerBlock and stolen.version() == previousVersion;
    }

    bool advanceProducer(Block &block, Cursor const committed) noexcept {
        Block &next = *block.next;
        Cursor const round = committed.nextRound(next.isHead);
        if (not isNextBlockWritable(next, round.version())) {
            return false;
        }
        next.committed.store(round.raw, std::memory_order_relaxed);
        next.stolen.store(round.raw, std::memory_order_relaxed);
        // `reserved` is published last: thieves read it with acquire and
        // then trust `committed` / `stolen` of the new round.
        next.reserved.store(round.raw, std::memory_order_release);
        producerBlock = &next;
        return true;
    }

    // --- consumer -----------------------------------------------------------

    // The producer has entered `next` for the consumer's next round.
    static bool consumerMayAdvance(Block const &next, Cursor const consumed) noexcept {
        std::size_t const nextVersion = consumed.version() + (next.isHead ? 1 : 0);
        Cursor const reserved{next.reserved.load(std::memory_order_relaxed)};
        return reserved.version() == nextVersion;
    }

    // Take `next` over from the thieves: fully reserve it, skip whatever they
    // already claimed, and credit the rest to `stolen` so the block still
    // reads as "all claims done" for the producer's reuse check.
    bool takeOverConsumerBlock(Block &next, Cursor const consumed) noexcept {
        if (not consumerMayAdvance(next, consumed)) {
            return false;
        }
        std::size_t const nextVersion = consumed.version() + (next.isHead ? 1 : 0);
        Cursor const fullyReserved = Cursor::make(nextVersion, EntriesPerBlock);
        Cursor const previous{next.reserved.exchange(fullyReserved.raw, std::memory_order_relaxed)};
        std::size_t const claimedByThieves = previous.index() > EntriesPerBlock ? EntriesPerBlock : previous.index();
        next.stolen.fetch_add(EntriesPerBlock - claimedByThieves, std::memory_order_relaxed);
        next.consumed.store(previous.raw, std::memory_order_relaxed);
        consumerBlock = &next;
        return true;
    }

    bool locateConsumer(Block *&outBlock, Cursor &outConsumed) noexcept {
        Block *block = consumerBlock;
        // NumBlocks + 1: after a full lap the starting block may be visited
        // again in a new round (everything in between was stolen).
        for (std::size_t i = 0; i < NumBlocks + 1; ++i) {
            Cursor const consumed{block->consumed.load(std::memory_order_relaxed)};
            if (consumed.index() < EntriesPerBlock) {
                Cursor const committed{block->committed.load(std::memory_order_relaxed)};
                if (consumed.index() == committed.index()) {
                    return false;
                }
                outBlock = block;
                outConsumed = consumed;
                return true;
            }
            Block &next = *block->next;
            if (not takeOverConsumerBlock(next, consumed)) {
                return false;
            }
            block = &next;
        }
        return false;
    }

    // --- thieves ------------------------------------------------------------

    static bool thiefMayAdvance(Block const &block, Cursor const reserved) noexcept {
        Block const &next = *block.next;
        std::size_t const expectedVersion = reserved.version() + (next.isHead ? 1 : 0);
        Cursor const nextReserved{next.reserved.load(std::memory_order_relaxed)};
        return nextReserved.version() == expectedVersion;
    }

    alignas(detail::kCacheLineSize) Block blocks[NumBlocks];
    alignas(detail::kCacheLineSize) Block *producerBlock = nullptr;
    alignas(detail::kCacheLineSize) Block *consumerBlock = nullptr;
    alignas(detail::kCacheLineSize) std::atomic<std::size_t> stealPosition{0};
};

// C++14: out-of-class definitions so the constants may be odr-used.
template <class T, std::size_t NumBlocks, std::size_t EntriesPerBlock>
constexpr std::size_t BwosQueue<T, NumBlocks, EntriesPerBlock>::kNumBlocks;
template <class T, std::size_t NumBlocks, std::size_t EntriesPerBlock>
constexpr std::size_t BwosQueue<T, NumBlocks, EntriesPerBlock>::kEntriesPerBlock;
template <class T, std::size_t NumBlocks, std::size_t EntriesPerBlock>
constexpr std::size_t BwosQueue<T, NumBlocks, EntriesPerBlock>::kCapacity;

} // namespace queue
} // namespace stackfull
