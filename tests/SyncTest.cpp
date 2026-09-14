#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/Channel.h>
#include <stackfull/sync/ConditionVariable.h>
#include <stackfull/sync/Mutex.h>
#include <stackfull/sync/Semaphore.h>
#include <stackfull/sync/WaitGroup.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace stackfull::sched;
using namespace stackfull::sync;

namespace {

std::unique_ptr<Scheduler> startScheduler(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    auto scheduler = makeScheduler(options);
    scheduler->start();
    return scheduler;
}

void spinFor(std::chrono::microseconds const duration) {
    auto const until = std::chrono::steady_clock::now() + duration;
    while (std::chrono::steady_clock::now() < until) {
    }
}

} // namespace

// ---------------------------------------------------------------------------
// Mutex
// ---------------------------------------------------------------------------

TEST(Mutex, ProvidesMutualExclusionAcrossWorkers) {
    auto scheduler = startScheduler(4);
    Mutex mutex;
    WaitGroup group;
    long counter = 0; // plain, protected only by `mutex`
    constexpr int kTasks = 16;
    constexpr int kIterations = 5000;
    group.add(kTasks);
    for (int t = 0; t < kTasks; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            for (int i = 0; i < kIterations; ++i) {
                LockGuard const guard(mutex);
                long const seen = counter;
                if ((i & 63) == 0) {
                    this_task::yield(); // hold the lock across a switch
                }
                counter = seen + 1;
            }
            group.done();
        }));
    }
    group.wait(); // main thread blocks on a task-driven WaitGroup
    EXPECT_EQ(counter, static_cast<long>(kTasks) * kIterations);
}

TEST(Mutex, TryLockFailsWhileHeldAndSucceedsAfterUnlock) {
    Mutex mutex;
    EXPECT_TRUE(mutex.tryLock());
    EXPECT_FALSE(mutex.tryLock());
    mutex.unlock();
    EXPECT_TRUE(mutex.tryLock());
    mutex.unlock();
}

TEST(Mutex, ThreadAndTasksContendForTheSameMutex) {
    auto scheduler = startScheduler(2);
    Mutex mutex;
    WaitGroup group;
    long counter = 0;
    group.add(4);
    for (int t = 0; t < 4; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            for (int i = 0; i < 2000; ++i) {
                LockGuard const guard(mutex);
                ++counter;
            }
            group.done();
        }));
    }
    for (int i = 0; i < 2000; ++i) {
        mutex.lock(); // plain thread: blocks on the Parker path
        ++counter;
        mutex.unlock();
    }
    group.wait();
    EXPECT_EQ(counter, 5 * 2000L);
}

// ---------------------------------------------------------------------------
// ConditionVariable
// ---------------------------------------------------------------------------

TEST(ConditionVariable, ProducerConsumerHandOff) {
    auto scheduler = startScheduler(3);
    Mutex mutex;
    ConditionVariable notEmpty;
    ConditionVariable notFull;
    std::deque<int> queue;
    constexpr int kItems = 20000;
    constexpr std::size_t kBound = 8;
    std::vector<int> received;
    WaitGroup group;
    group.add(2);

    ASSERT_TRUE(scheduler->spawn([&] {
        for (int i = 0; i < kItems; ++i) {
            LockGuard const guard(mutex);
            notFull.wait(mutex, [&] { return queue.size() < kBound; });
            queue.push_back(i);
            notEmpty.notifyOne();
        }
        group.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        for (int i = 0; i < kItems; ++i) {
            LockGuard const guard(mutex);
            notEmpty.wait(mutex, [&] { return not queue.empty(); });
            received.push_back(queue.front());
            queue.pop_front();
            notFull.notifyOne();
        }
        group.done();
    }));
    group.wait();
    ASSERT_EQ(received.size(), static_cast<std::size_t>(kItems));
    for (int i = 0; i < kItems; ++i) {
        ASSERT_EQ(received[static_cast<std::size_t>(i)], i);
    }
}

TEST(ConditionVariable, NotifyAllReleasesEveryWaiter) {
    auto scheduler = startScheduler(4);
    Mutex mutex;
    ConditionVariable go;
    bool ready = false;
    std::atomic<int> released{0};
    WaitGroup group;
    constexpr int kWaiters = 12;
    group.add(kWaiters);
    for (int t = 0; t < kWaiters; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            {
                LockGuard const guard(mutex);
                go.wait(mutex, [&] { return ready; });
            }
            released.fetch_add(1);
            group.done();
        }));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    EXPECT_EQ(released.load(), 0);
    {
        LockGuard const guard(mutex);
        ready = true;
    }
    go.notifyAll();
    group.wait();
    EXPECT_EQ(released.load(), kWaiters);
}

// ---------------------------------------------------------------------------
// Semaphore
// ---------------------------------------------------------------------------

TEST(Semaphore, LimitsConcurrency) {
    auto scheduler = startScheduler(4);
    Semaphore permits(3);
    std::atomic<int> inside{0};
    std::atomic<int> maxInside{0};
    WaitGroup group;
    constexpr int kTasks = 24;
    group.add(kTasks);
    for (int t = 0; t < kTasks; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            for (int i = 0; i < 50; ++i) {
                permits.acquire();
                int const now = inside.fetch_add(1) + 1;
                int seen = maxInside.load();
                while (now > seen and not maxInside.compare_exchange_weak(seen, now)) {
                }
                spinFor(std::chrono::microseconds{5});
                inside.fetch_sub(1);
                permits.release();
            }
            group.done();
        }));
    }
    group.wait();
    EXPECT_LE(maxInside.load(), 3);
    EXPECT_GE(maxInside.load(), 2); // it did run in parallel
    EXPECT_EQ(permits.available(), 3u);
}

TEST(Semaphore, ReleaseHandsPermitsToWaitersInOrder) {
    auto scheduler = startScheduler(1);
    Semaphore permits(0);
    std::vector<int> order;
    WaitGroup group;
    group.add(3);
    for (int t = 0; t < 3; ++t) {
        ASSERT_TRUE(scheduler->spawn([&, t] {
            permits.acquire();
            order.push_back(t);
            group.done();
        }));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{10}); // all three parked
    for (int t = 0; t < 3; ++t) {
        permits.release();
    }
    group.wait();
    std::vector<int> const expected{0, 1, 2};
    EXPECT_EQ(order, expected);
}

// ---------------------------------------------------------------------------
// WaitGroup
// ---------------------------------------------------------------------------

TEST(WaitGroup, WaitFromTaskAndFromThread) {
    auto scheduler = startScheduler(2);
    WaitGroup workers;
    WaitGroup observer;
    std::atomic<int> finished{0};
    bool observerSawAll = false;
    workers.add(5);
    observer.add(1);
    ASSERT_TRUE(scheduler->spawn([&] {
        workers.wait(); // task-side wait
        observerSawAll = finished.load() == 5;
        observer.done();
    }));
    for (int t = 0; t < 5; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            spinFor(std::chrono::microseconds{200});
            finished.fetch_add(1);
            workers.done();
        }));
    }
    observer.wait(); // thread-side wait
    EXPECT_TRUE(observerSawAll);
    EXPECT_EQ(workers.pendingCount(), 0u);
}

TEST(WaitGroup, WaitOnZeroReturnsImmediately) {
    WaitGroup group;
    group.wait();
    group.add(1);
    group.done();
    group.wait();
}

// ---------------------------------------------------------------------------
// Channel
// ---------------------------------------------------------------------------

TEST(Channel, SingleProducerSingleConsumerPreservesOrder) {
    auto scheduler = startScheduler(2);
    Channel<int> channel(4);
    std::vector<int> received;
    WaitGroup group;
    group.add(2);
    constexpr int kItems = 50000;
    ASSERT_TRUE(scheduler->spawn([&] {
        for (int i = 0; i < kItems; ++i) {
            ASSERT_TRUE(channel.send(i));
        }
        channel.close();
        group.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        int value = 0;
        while (channel.recv(value)) {
            received.push_back(value);
        }
        group.done();
    }));
    group.wait();
    ASSERT_EQ(received.size(), static_cast<std::size_t>(kItems));
    for (int i = 0; i < kItems; ++i) {
        ASSERT_EQ(received[static_cast<std::size_t>(i)], i);
    }
}

TEST(Channel, ManyProducersManyConsumersDeliverEverythingOnce) {
    auto scheduler = startScheduler(4);
    Channel<int> channel(16);
    constexpr int kProducers = 4;
    constexpr int kConsumers = 3;
    constexpr int kPerProducer = 10000;
    std::mutex sink;
    std::vector<int> received;
    WaitGroup producers;
    WaitGroup consumers;
    producers.add(kProducers);
    consumers.add(kConsumers);
    for (int p = 0; p < kProducers; ++p) {
        ASSERT_TRUE(scheduler->spawn([&, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                ASSERT_TRUE(channel.send(p * kPerProducer + i));
            }
            producers.done();
        }));
    }
    for (int c = 0; c < kConsumers; ++c) {
        ASSERT_TRUE(scheduler->spawn([&] {
            int value = 0;
            while (channel.recv(value)) {
                std::lock_guard<std::mutex> const guard(sink);
                received.push_back(value);
            }
            consumers.done();
        }));
    }
    producers.wait();
    channel.close();
    consumers.wait();
    std::sort(received.begin(), received.end());
    ASSERT_EQ(received.size(), static_cast<std::size_t>(kProducers * kPerProducer));
    for (std::size_t i = 0; i < received.size(); ++i) {
        ASSERT_EQ(received[i], static_cast<int>(i));
    }
}

TEST(Channel, CloseWakesBlockedSendersAndReceivers) {
    auto scheduler = startScheduler(2);
    Channel<int> channel(1);
    std::atomic<int> sendFailed{0};
    std::atomic<int> recvFailed{0};
    WaitGroup group;
    group.add(3);
    ASSERT_TRUE(channel.trySend(1)); // full from now on
    ASSERT_TRUE(scheduler->spawn([&] {
        if (not channel.send(2)) {
            sendFailed.fetch_add(1);
        }
        group.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        int value = 0;
        ASSERT_TRUE(channel.recv(value)); // drains the buffered 1 (or 2, if the sender got in first)
        while (channel.recv(value)) {
        }
        recvFailed.fetch_add(1);
        group.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        int value = 0;
        while (channel.recv(value)) {
        }
        recvFailed.fetch_add(1);
        group.done();
    }));
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    channel.close();
    group.wait();
    EXPECT_EQ(recvFailed.load(), 2);
    EXPECT_FALSE(channel.send(3));
    EXPECT_TRUE(channel.isClosed());
}

TEST(Channel, MoveOnlyPayloadsAndDestructorDrainsLeftovers) {
    struct Counter {
        explicit Counter(std::atomic<int> &alive_) : alive(&alive_) { alive->fetch_add(1); }
        Counter(Counter &&other) noexcept : alive(other.alive) { other.alive = nullptr; }
        Counter &operator=(Counter &&other) noexcept {
            if (alive != nullptr) {
                alive->fetch_sub(1);
            }
            alive = other.alive;
            other.alive = nullptr;
            return *this;
        }
        ~Counter() {
            if (alive != nullptr) {
                alive->fetch_sub(1);
            }
        }
        std::atomic<int> *alive;
    };
    std::atomic<int> alive{0};
    {
        Channel<Counter> channel(8);
        for (int i = 0; i < 5; ++i) {
            ASSERT_TRUE(channel.trySend(Counter{alive}));
        }
        EXPECT_EQ(alive.load(), 5);
        Counter taken{alive};
        ASSERT_TRUE(channel.tryRecv(taken));
        EXPECT_EQ(alive.load(), 5); // the assigned-over element died, the received one lives
    }
    EXPECT_EQ(alive.load(), 0);
}

TEST(Channel, UsableFromPlainThreadsToo) {
    auto scheduler = startScheduler(2);
    Channel<std::string> channel(2);
    WaitGroup group;
    group.add(1);
    std::vector<std::string> received;
    ASSERT_TRUE(scheduler->spawn([&] {
        std::string value;
        while (channel.recv(value)) {
            received.push_back(value);
        }
        group.done();
    }));
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(channel.send(std::to_string(i))); // thread-side send, parks the thread when full
    }
    channel.close();
    group.wait();
    ASSERT_EQ(received.size(), 100u);
    EXPECT_EQ(received.front(), "0");
    EXPECT_EQ(received.back(), "99");
}
