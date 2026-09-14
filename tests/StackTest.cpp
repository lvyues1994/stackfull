#include <stackfull/stack/DefaultStackAllocator.h>
#include <stackfull/stack/MmapStackAllocator.h>
#include <stackfull/stack/PooledStackAllocator.h>
#include <stackfull/stack/StackAllocator.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <thread>

using namespace stackfull::stack;

namespace {

std::uintptr_t addressOf(void const *const p) noexcept {
    return reinterpret_cast<std::uintptr_t>(p);
}

// Upstream double that counts traffic so pool behaviour is observable.
struct CountingAllocator final : StackAllocator {
    StackAllocation allocate(std::size_t const size) noexcept override {
        allocations.fetch_add(1);
        return upstream->allocate(size);
    }
    void deallocate(StackView const &stack) noexcept override {
        deallocations.fetch_add(1);
        upstream->deallocate(stack);
    }

    std::unique_ptr<StackAllocator> upstream = makeMmapStackAllocator();
    std::atomic<int> allocations{0};
    std::atomic<int> deallocations{0};
};

struct FailingAllocator final : StackAllocator {
    StackAllocation allocate(std::size_t) noexcept override {
        return StackAllocation{StackView{}, std::make_error_code(std::errc::not_enough_memory)};
    }
    void deallocate(StackView const &) noexcept override {}
};

} // namespace

TEST(MmapStackAllocator, RoundsUpToPageAndAlignsTop) {
    auto const allocator = makeMmapStackAllocator();
    StackAllocation const allocation = allocator->allocate(1);
    ASSERT_TRUE(allocation);
    EXPECT_EQ(allocation.stack.size, pageSize());
    EXPECT_EQ(addressOf(allocation.stack.base) % pageSize(), 0u);
    EXPECT_EQ(addressOf(topOf(allocation.stack)) % 16, 0u);

    std::memset(allocation.stack.base, 0xAB, allocation.stack.size); // whole view writable
    allocator->deallocate(allocation.stack);
}

TEST(MmapStackAllocator, RejectsZeroSize) {
    auto const allocator = makeMmapStackAllocator();
    StackAllocation const allocation = allocator->allocate(0);
    EXPECT_FALSE(allocation);
    EXPECT_EQ(allocation.error, std::errc::invalid_argument);
    EXPECT_TRUE(isEmpty(allocation.stack));
}

TEST(MmapStackAllocator, GuardPageFaultsOnOverflow) {
    auto const allocator = makeMmapStackAllocator();
    StackAllocation const allocation = allocator->allocate(pageSize());
    ASSERT_TRUE(allocation);
    volatile char *const belowBase = static_cast<char *>(allocation.stack.base) - 1;
    EXPECT_DEATH_IF_SUPPORTED({ *belowBase = 1; }, "");
    allocator->deallocate(allocation.stack);
}

TEST(MmapStackAllocator, NoGuardWhenDisabled) {
    MmapStackOptions options;
    options.guardPages = 0;
    auto const allocator = makeMmapStackAllocator(options);
    StackAllocation const allocation = allocator->allocate(pageSize());
    ASSERT_TRUE(allocation);
    allocator->deallocate(allocation.stack);
}

TEST(PooledStackAllocator, ReusesMostRecentlyReleasedStack) {
    CountingAllocator upstream;
    auto const pooled = makePooledStackAllocator(upstream);

    StackAllocation const first = pooled->allocate(16 * 1024);
    StackAllocation const second = pooled->allocate(16 * 1024);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(upstream.allocations.load(), 2);

    pooled->deallocate(first.stack);
    pooled->deallocate(second.stack);
    EXPECT_EQ(upstream.deallocations.load(), 0);

    StackAllocation const third = pooled->allocate(16 * 1024); // LIFO: gets `second`
    EXPECT_EQ(third.stack.base, second.stack.base);
    StackAllocation const fourth = pooled->allocate(16 * 1024);
    EXPECT_EQ(fourth.stack.base, first.stack.base);
    EXPECT_EQ(upstream.allocations.load(), 2);

    pooled->deallocate(third.stack);
    pooled->deallocate(fourth.stack);
}

TEST(PooledStackAllocator, DifferentSizesUseDifferentClasses) {
    CountingAllocator upstream;
    auto const pooled = makePooledStackAllocator(upstream);

    StackAllocation const small = pooled->allocate(16 * 1024);
    StackAllocation const large = pooled->allocate(64 * 1024);
    pooled->deallocate(small.stack);
    pooled->deallocate(large.stack);

    StackAllocation const again = pooled->allocate(64 * 1024);
    EXPECT_EQ(again.stack.base, large.stack.base);
    EXPECT_EQ(again.stack.size, large.stack.size);
    pooled->deallocate(again.stack);
}

TEST(PooledStackAllocator, OverflowGoesToTheSharedTierNotUpstream) {
    CountingAllocator upstream;
    PooledStackOptions options;
    options.maxCachedStacksPerThread = 1;
    auto const pooled = makePooledStackAllocator(upstream, options);

    StackAllocation const first = pooled->allocate(16 * 1024);
    StackAllocation const second = pooled->allocate(16 * 1024);
    pooled->deallocate(first.stack);  // thread cache
    pooled->deallocate(second.stack); // over the per-thread limit → shared tier
    EXPECT_EQ(upstream.deallocations.load(), 0);

    // Both come back without touching upstream again.
    StackAllocation const a = pooled->allocate(16 * 1024);
    StackAllocation const b = pooled->allocate(16 * 1024);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_EQ(upstream.allocations.load(), 2);
    pooled->deallocate(a.stack);
    pooled->deallocate(b.stack);
}

// The scheduler pattern: one thread allocates, another releases.
TEST(PooledStackAllocator, ProducerConsumerThreadsRecycleThroughTheSharedTier) {
    CountingAllocator upstream;
    auto const pooled = makePooledStackAllocator(upstream);
    constexpr int kRounds = 2000;
    std::vector<StackView> handoff(kRounds);
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};

    std::thread consumer([&] {
        while (consumed.load() < kRounds) {
            int const next = consumed.load();
            if (produced.load(std::memory_order_acquire) > next) {
                pooled->deallocate(handoff[static_cast<std::size_t>(next)]);
                consumed.fetch_add(1);
            }
        }
    });
    for (int i = 0; i < kRounds; ++i) {
        while (produced.load() - consumed.load() >= 32) {
        }
        StackAllocation const allocation = pooled->allocate(16 * 1024);
        ASSERT_TRUE(allocation);
        handoff[static_cast<std::size_t>(i)] = allocation.stack;
        produced.fetch_add(1, std::memory_order_release);
    }
    consumer.join();
    // Far fewer than one mmap per round: the shared tier recycled them.
    EXPECT_LT(upstream.allocations.load(), kRounds / 4);
}

TEST(PooledStackAllocator, DestructionDrainsEveryThreadCache) {
    CountingAllocator upstream;
    {
        auto const pooled = makePooledStackAllocator(upstream);
        StackAllocation const a = pooled->allocate(16 * 1024);
        pooled->deallocate(a.stack);
        std::thread([&] {
            StackAllocation const b = pooled->allocate(32 * 1024);
            pooled->deallocate(b.stack);
        }).join(); // thread exit drains its own cache
        EXPECT_EQ(upstream.deallocations.load(), 1);
    }
    EXPECT_EQ(upstream.allocations.load(), upstream.deallocations.load());
}

TEST(PooledStackAllocator, StackReleasedOnAnotherThreadIsReusedThere) {
    CountingAllocator upstream;
    auto const pooled = makePooledStackAllocator(upstream);

    StackAllocation const fromMain = pooled->allocate(16 * 1024);
    void *reused = nullptr;
    std::thread([&] {
        pooled->deallocate(fromMain.stack);
        StackAllocation const local = pooled->allocate(16 * 1024);
        reused = local.stack.base;
        pooled->deallocate(local.stack);
    }).join();
    EXPECT_EQ(reused, fromMain.stack.base);
    EXPECT_EQ(upstream.allocations.load(), 1);
}

TEST(PooledStackAllocator, PropagatesUpstreamFailure) {
    FailingAllocator upstream;
    auto const pooled = makePooledStackAllocator(upstream);
    StackAllocation const allocation = pooled->allocate(16 * 1024);
    EXPECT_FALSE(allocation);
    EXPECT_EQ(allocation.error, std::errc::not_enough_memory);
}

TEST(DefaultStackAllocator, IsASingleUsableInstance) {
    StackAllocator &a = defaultStackAllocator();
    StackAllocator &b = defaultStackAllocator();
    EXPECT_EQ(&a, &b);
    StackAllocation const allocation = a.allocate(16 * 1024);
    ASSERT_TRUE(allocation);
    a.deallocate(allocation.stack);
}
