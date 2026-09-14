#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/Sleep.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/WaitGroup.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

using namespace stackfull::sched;
using stackfull::sync::WaitGroup;
using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;

namespace {

std::unique_ptr<Scheduler> startScheduler(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    auto scheduler = makeScheduler(options);
    scheduler->start();
    return scheduler;
}

milliseconds elapsedSince(Clock::time_point const start) {
    return std::chrono::duration_cast<milliseconds>(Clock::now() - start);
}

} // namespace

TEST(Timer, SleepForWaitsAtLeastTheRequestedTime) {
    auto scheduler = startScheduler(2);
    WaitGroup done;
    done.add(1);
    milliseconds slept{0};
    ASSERT_TRUE(scheduler->spawn([&] {
        auto const start = Clock::now();
        this_task::sleepFor(milliseconds{20});
        slept = elapsedSince(start);
        done.done();
    }));
    done.wait();
    EXPECT_GE(slept.count(), 20);
    EXPECT_LT(slept.count(), 20 + 30); // generous: CI machines
}

TEST(Timer, SleepersWakeInDeadlineOrder) {
    auto scheduler = startScheduler(4);
    WaitGroup done;
    std::mutex mutex;
    std::vector<int> order;
    int const delays[] = {30, 10, 50, 20, 40};
    done.add(5);
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(scheduler->spawn([&, i] {
            this_task::sleepFor(milliseconds{delays[i]});
            std::lock_guard<std::mutex> const guard(mutex);
            order.push_back(delays[i]);
            done.done();
        }));
    }
    done.wait();
    std::vector<int> const expected{10, 20, 30, 40, 50};
    EXPECT_EQ(order, expected);
}

TEST(Timer, ManySleepersAllWake) {
    auto scheduler = startScheduler(4);
    constexpr int kSleepers = 2000;
    WaitGroup done;
    std::atomic<int> woke{0};
    done.add(kSleepers);
    auto const start = Clock::now();
    std::mt19937 rng(3);
    for (int i = 0; i < kSleepers; ++i) {
        int const ms = 1 + static_cast<int>(rng() % 20);
        ASSERT_TRUE(scheduler->spawn([&, ms] {
            this_task::sleepFor(milliseconds{ms});
            woke.fetch_add(1);
            done.done();
        }));
    }
    done.wait();
    EXPECT_EQ(woke.load(), kSleepers);
    EXPECT_LT(elapsedSince(start).count(), 200);
}

// A worker that never yields cannot fire timers itself; an idle peer must.
TEST(Timer, FiresWhileAnotherWorkerIsBusy) {
    auto scheduler = startScheduler(2);
    WaitGroup done;
    done.add(2);
    std::atomic<bool> stopSpinning{false};
    milliseconds slept{0};
    ASSERT_TRUE(scheduler->spawn([&] {
        while (not stopSpinning.load()) {
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        auto const start = Clock::now();
        this_task::sleepFor(milliseconds{10});
        slept = elapsedSince(start);
        stopSpinning.store(true);
        done.done();
    }));
    done.wait();
    EXPECT_GE(slept.count(), 10);
    EXPECT_LT(slept.count(), 10 + 30);
}

// The timekeeper sleeps with the earliest deadline as its timeout; a new,
// earlier timer must cut that sleep short.
TEST(Timer, EarlierDeadlineShortensTheTimekeepersSleep) {
    auto scheduler = startScheduler(2);
    WaitGroup done;
    done.add(2);
    milliseconds shortSleep{0};
    ASSERT_TRUE(scheduler->spawn([&] {
        this_task::sleepFor(milliseconds{300});
        done.done();
    }));
    std::this_thread::sleep_for(milliseconds{20}); // let the long sleeper become the only timer
    auto const start = Clock::now();
    ASSERT_TRUE(scheduler->spawn([&] {
        this_task::sleepFor(milliseconds{10});
        shortSleep = elapsedSince(start);
        done.done();
    }));
    done.wait();
    EXPECT_GE(shortSleep.count(), 10);
    EXPECT_LT(shortSleep.count(), 10 + 40);
}

TEST(Timer, SleepFromTaskYieldingLoopDoesNotStarveTimers) {
    auto scheduler = startScheduler(1);
    WaitGroup done;
    done.add(2);
    std::atomic<bool> stop{false};
    milliseconds slept{0};
    ASSERT_TRUE(scheduler->spawn([&] {
        while (not stop.load()) {
            this_task::yield(); // busy but cooperative on the only worker
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        auto const start = Clock::now();
        this_task::sleepFor(milliseconds{10});
        slept = elapsedSince(start);
        stop.store(true);
        done.done();
    }));
    done.wait();
    EXPECT_GE(slept.count(), 10);
    EXPECT_LT(slept.count(), 10 + 50);
}
