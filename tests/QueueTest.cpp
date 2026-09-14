#include <stackfull/queue/BbqQueue.h>
#include <stackfull/queue/BwosQueue.h>
#include <stackfull/queue/RingQueue.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <random>
#include <thread>
#include <vector>

using namespace stackfull::queue;

// ---------------------------------------------------------------------------
// Owner interface shared by BwosQueue and RingQueue
// ---------------------------------------------------------------------------

template <class Q>
struct OwnerQueueTest : ::testing::Test {
    Q queue;
};

using OwnerQueues = ::testing::Types<BwosQueue<int, 4, 4>, RingQueue<int, 16>, BwosQueue<int, 8, 32>>;
TYPED_TEST_SUITE(OwnerQueueTest, OwnerQueues);

TYPED_TEST(OwnerQueueTest, StartsEmpty) {
    int value = 0;
    EXPECT_FALSE(this->queue.hasEntries());
    EXPECT_FALSE(this->queue.pop(value));
    EXPECT_FALSE(this->queue.steal(value));
    EXPECT_TRUE(this->queue.isEmptyForThief());
}

TYPED_TEST(OwnerQueueTest, PopsInFifoOrder) {
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(this->queue.push(i));
    }
    EXPECT_TRUE(this->queue.hasEntries());
    for (int i = 0; i < 5; ++i) {
        int value = -1;
        ASSERT_TRUE(this->queue.pop(value));
        EXPECT_EQ(value, i);
    }
    int value = 0;
    EXPECT_FALSE(this->queue.pop(value));
}

TYPED_TEST(OwnerQueueTest, ReportsFullAtDocumentedCapacity) {
    int pushed = 0;
    while (this->queue.push(pushed)) {
        ++pushed;
        ASSERT_LT(pushed, 100000) << "queue never reports full";
    }
    EXPECT_GE(static_cast<std::size_t>(pushed), TypeParam::kCapacity);
    // Draining and refilling works after the full condition.
    int value = 0;
    for (int i = 0; i < pushed; ++i) {
        ASSERT_TRUE(this->queue.pop(value));
        EXPECT_EQ(value, i);
    }
    EXPECT_FALSE(this->queue.pop(value));
    ASSERT_TRUE(this->queue.push(42));
    ASSERT_TRUE(this->queue.pop(value));
    EXPECT_EQ(value, 42);
}

TYPED_TEST(OwnerQueueTest, SurvivesManyWrapArounds) {
    int next = 0;
    int expected = 0;
    std::mt19937 rng(7);
    for (int round = 0; round < 2000; ++round) {
        int const burst = static_cast<int>(rng() % 7);
        for (int i = 0; i < burst; ++i) {
            if (this->queue.push(next)) {
                ++next;
            }
        }
        int const drain = static_cast<int>(rng() % 7);
        for (int i = 0; i < drain; ++i) {
            int value = -1;
            if (this->queue.pop(value)) {
                ASSERT_EQ(value, expected);
                ++expected;
            }
        }
    }
    int value = -1;
    while (this->queue.pop(value)) {
        ASSERT_EQ(value, expected);
        ++expected;
    }
    EXPECT_EQ(expected, next);
}

TYPED_TEST(OwnerQueueTest, MatchesDequeModelUnderRandomOwnerOps) {
    std::deque<int> model;
    std::mt19937 rng(11);
    int next = 0;
    int out[TypeParam::kEntriesPerBlock];
    for (int step = 0; step < 20000; ++step) {
        switch (rng() % 4) {
        case 0:
        case 1: {
            bool const ok = this->queue.push(next);
            if (ok) {
                model.push_back(next);
            }
            ++next;
            break;
        }
        case 2: {
            int value = -1;
            bool const ok = this->queue.pop(value);
            ASSERT_EQ(ok, not model.empty());
            if (ok) {
                ASSERT_EQ(value, model.front());
                model.pop_front();
            }
            break;
        }
        case 3: {
            std::size_t const n = this->queue.popBlock(out);
            ASSERT_LE(n, model.size());
            for (std::size_t i = 0; i < n; ++i) {
                ASSERT_EQ(out[i], model.front());
                model.pop_front();
            }
            break;
        }
        }
        ASSERT_EQ(this->queue.hasEntries(), not model.empty());
    }
}

TYPED_TEST(OwnerQueueTest, PushBatchRespectsRemainingSlots) {
    std::vector<int> batch;
    for (int i = 0; i < 100; ++i) {
        batch.push_back(i);
    }
    std::size_t const room = this->queue.minRemainingSlots();
    std::size_t const pushed = this->queue.pushBatch(batch.begin(), batch.end());
    EXPECT_GE(pushed, std::min<std::size_t>(room, batch.size()));
    for (std::size_t i = 0; i < pushed; ++i) {
        int value = -1;
        ASSERT_TRUE(this->queue.pop(value));
        EXPECT_EQ(value, static_cast<int>(i));
    }
}

// ---------------------------------------------------------------------------
// BWoS-specific single-thread semantics
// ---------------------------------------------------------------------------

TEST(BwosQueue, ConsumerBlockIsNotStealable) {
    BwosQueue<int, 4, 4> queue;
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(queue.push(i)); // all in block 0 == consumer block
    }
    int value = -1;
    EXPECT_FALSE(queue.hasStealable());
    EXPECT_FALSE(queue.steal(value));

    ASSERT_TRUE(queue.push(4)); // producer advanced to block 1
    EXPECT_TRUE(queue.hasStealable());
    ASSERT_TRUE(queue.steal(value));
    EXPECT_EQ(value, 4);
    EXPECT_FALSE(queue.steal(value));
}

TEST(BwosQueue, ConsumerSkipsStolenEntriesWhenTakingOverABlock) {
    BwosQueue<int, 4, 4> queue;
    for (int i = 0; i < 8; ++i) {
        ASSERT_TRUE(queue.push(i));
    }
    int value = -1;
    ASSERT_TRUE(queue.steal(value));
    EXPECT_EQ(value, 4);
    ASSERT_TRUE(queue.steal(value));
    EXPECT_EQ(value, 5);

    std::vector<int> popped;
    while (queue.pop(value)) {
        popped.push_back(value);
    }
    std::vector<int> const expected{0, 1, 2, 3, 6, 7};
    EXPECT_EQ(popped, expected);
}

TEST(BwosQueue, StealBlockTakesTheWholeCommittedRange) {
    BwosQueue<int, 4, 4> queue;
    for (int i = 0; i < 7; ++i) {
        ASSERT_TRUE(queue.push(i));
    }
    int out[4] = {-1, -1, -1, -1};
    std::size_t const n = queue.stealBlock(out);
    ASSERT_EQ(n, 3u); // block 1 holds 4, 5, 6
    EXPECT_EQ(out[0], 4);
    EXPECT_EQ(out[2], 6);
    EXPECT_EQ(queue.stealBlock(out), 0u);
}

// The consumer only advances on pop(), so after a round it may sit one block
// behind the data: which items thieves get and which the owner gets varies,
// but every item is delivered exactly once and blocks keep being recycled.
TEST(BwosQueue, EverythingStolenThenProducerRecyclesBlocks) {
    BwosQueue<int, 4, 2> queue;
    int value = -1;
    for (int round = 0; round < 50; ++round) {
        std::vector<int> delivered;
        for (int i = 0; i < 4; ++i) {
            ASSERT_TRUE(queue.push(round * 10 + i));
        }
        while (queue.steal(value)) {
            delivered.push_back(value);
        }
        EXPECT_GE(delivered.size(), 2u); // at least the block(s) beyond the consumer's
        while (queue.pop(value)) {
            delivered.push_back(value);
        }
        std::sort(delivered.begin(), delivered.end());
        std::vector<int> const expected{round * 10, round * 10 + 1, round * 10 + 2, round * 10 + 3};
        EXPECT_EQ(delivered, expected);
    }
}

// ---------------------------------------------------------------------------
// Concurrent stress: owner + thieves, every item observed exactly once
// ---------------------------------------------------------------------------

namespace {

template <class Q>
void ownerThiefStress(int const itemCount, int const thiefCount) {
    Q queue;
    std::atomic<bool> producing{true};
    std::vector<std::vector<int>> seenByThief(static_cast<std::size_t>(thiefCount));
    std::vector<int> seenByOwner;
    std::vector<std::thread> thieves;

    for (int t = 0; t < thiefCount; ++t) {
        thieves.emplace_back([&, t] {
            std::vector<int> &seen = seenByThief[static_cast<std::size_t>(t)];
            int buffer[Q::kEntriesPerBlock];
            std::mt19937 rng(static_cast<unsigned>(t + 1));
            for (;;) {
                if (rng() % 4 == 0) {
                    std::size_t const n = queue.stealBlock(buffer);
                    seen.insert(seen.end(), buffer, buffer + n);
                } else {
                    int value = -1;
                    if (queue.steal(value)) {
                        seen.push_back(value);
                    }
                }
                if (not producing.load(std::memory_order_acquire) and queue.isEmptyForThief()) {
                    break;
                }
            }
        });
    }

    std::mt19937 rng(99);
    int buffer[Q::kEntriesPerBlock];
    for (int i = 0; i < itemCount;) {
        if (queue.push(i)) {
            ++i;
        } else if (rng() % 2 == 0) {
            int value = -1;
            if (queue.pop(value)) {
                seenByOwner.push_back(value);
            }
        } else {
            std::size_t const n = queue.popBlock(buffer);
            seenByOwner.insert(seenByOwner.end(), buffer, buffer + n);
        }
        if (rng() % 3 == 0) {
            int value = -1;
            if (queue.pop(value)) {
                seenByOwner.push_back(value);
            }
        }
    }
    int value = -1;
    while (queue.pop(value)) {
        seenByOwner.push_back(value);
    }
    producing.store(false, std::memory_order_release);
    for (std::thread &thief : thieves) {
        thief.join();
    }
    while (queue.pop(value)) {
        seenByOwner.push_back(value);
    }

    std::vector<int> all = seenByOwner;
    for (auto const &seen : seenByThief) {
        all.insert(all.end(), seen.begin(), seen.end());
    }
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), static_cast<std::size_t>(itemCount));
    for (int i = 0; i < itemCount; ++i) {
        ASSERT_EQ(all[static_cast<std::size_t>(i)], i) << "item lost or duplicated";
    }
}

} // namespace

TEST(BwosQueueStress, OwnerAndThievesSeeEveryItemOnce) {
    ownerThiefStress<BwosQueue<int, 8, 32>>(200000, 3);
}

TEST(BwosQueueStress, TinyBlocksMaximiseBlockTransitions) {
    ownerThiefStress<BwosQueue<int, 4, 2>>(100000, 4);
}

TEST(RingQueueStress, OwnerAndThievesSeeEveryItemOnce) {
    ownerThiefStress<RingQueue<int, 256>>(200000, 3);
}

// ---------------------------------------------------------------------------
// BBQ
// ---------------------------------------------------------------------------

TEST(BbqQueue, SingleThreadFifo) {
    BbqQueue<int, 4, 4> queue;
    int value = -1;
    EXPECT_EQ(queue.pop(value), PopStatus::Empty);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(queue.push(i), PushStatus::Ok);
    }
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(queue.pop(value), PopStatus::Ok);
        EXPECT_EQ(value, i);
    }
    EXPECT_EQ(queue.pop(value), PopStatus::Empty);
}

TEST(BbqQueue, ReportsFullAndRecovers) {
    using Queue = BbqQueue<int, 4, 4>;
    Queue queue;
    int pushed = 0;
    while (queue.push(pushed) == PushStatus::Ok) {
        ++pushed;
        ASSERT_LT(pushed, 1000);
    }
    EXPECT_GE(static_cast<std::size_t>(pushed), Queue::kCapacity);
    EXPECT_EQ(queue.push(pushed), PushStatus::Full);
    int value = -1;
    for (int i = 0; i < pushed; ++i) {
        ASSERT_EQ(queue.pop(value), PopStatus::Ok);
        EXPECT_EQ(value, i);
    }
    EXPECT_EQ(queue.pop(value), PopStatus::Empty);
    ASSERT_EQ(queue.push(7), PushStatus::Ok);
    ASSERT_EQ(queue.pop(value), PopStatus::Ok);
    EXPECT_EQ(value, 7);
}

TEST(BbqQueue, SurvivesManyWrapArounds) {
    BbqQueue<int, 4, 4> queue;
    int next = 0;
    int expected = 0;
    std::mt19937 rng(5);
    for (int round = 0; round < 5000; ++round) {
        int const burst = static_cast<int>(rng() % 6);
        for (int i = 0; i < burst; ++i) {
            if (queue.push(next) == PushStatus::Ok) {
                ++next;
            }
        }
        int const drain = static_cast<int>(rng() % 6);
        for (int i = 0; i < drain; ++i) {
            int value = -1;
            if (queue.pop(value) == PopStatus::Ok) {
                ASSERT_EQ(value, expected);
                ++expected;
            }
        }
    }
    int value = -1;
    while (queue.pop(value) == PopStatus::Ok) {
        ASSERT_EQ(value, expected);
        ++expected;
    }
    EXPECT_EQ(expected, next);
}

namespace {

template <class Q>
void mpmcStress(int const producers, int const consumers, int const itemsPerProducer) {
    Q queue;
    std::atomic<int> remaining{producers * itemsPerProducer};
    std::vector<std::vector<int>> seen(static_cast<std::size_t>(consumers));
    std::vector<std::thread> threads;

    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&, p] {
            for (int i = 0; i < itemsPerProducer; ++i) {
                int const item = p * itemsPerProducer + i;
                for (;;) {
                    PushStatus const status = queue.push(item);
                    if (status == PushStatus::Ok) {
                        break;
                    }
                    std::this_thread::yield();
                }
            }
        });
    }
    for (int c = 0; c < consumers; ++c) {
        threads.emplace_back([&, c] {
            std::vector<int> &mine = seen[static_cast<std::size_t>(c)];
            while (remaining.load(std::memory_order_acquire) > 0) {
                int value = -1;
                PopStatus const status = queue.pop(value);
                if (status == PopStatus::Ok) {
                    mine.push_back(value);
                    remaining.fetch_sub(1, std::memory_order_acq_rel);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    for (std::thread &thread : threads) {
        thread.join();
    }

    std::vector<int> all;
    for (auto const &mine : seen) {
        all.insert(all.end(), mine.begin(), mine.end());
    }
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), static_cast<std::size_t>(producers * itemsPerProducer));
    for (std::size_t i = 0; i < all.size(); ++i) {
        ASSERT_EQ(all[i], static_cast<int>(i)) << "item lost or duplicated";
    }
}

} // namespace

TEST(BbqQueueStress, ManyProducersManyConsumers) {
    mpmcStress<BbqQueue<int, 8, 64>>(4, 4, 50000);
}

TEST(BbqQueueStress, TinyBlocksUnderContention) {
    mpmcStress<BbqQueue<int, 2, 2>>(4, 3, 20000);
}

TEST(BbqQueueStress, SingleProducerManyConsumers) {
    mpmcStress<BbqQueue<int, 16, 4096>>(1, 6, 200000);
}
