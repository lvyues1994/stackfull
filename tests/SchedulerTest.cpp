#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#include <stdexcept>
#endif

using namespace stackfull::sched;

namespace {

SchedulerOptions withWorkers(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    return options;
}

// Poll until no task is alive (or fail after `timeout`).
bool waitIdle(Scheduler &scheduler, std::chrono::milliseconds const timeout = std::chrono::seconds{10}) {
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (scheduler.liveTasks() != 0) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::microseconds{200});
    }
    return true;
}

void spinFor(std::chrono::microseconds const duration) {
    auto const until = std::chrono::steady_clock::now() + duration;
    while (std::chrono::steady_clock::now() < until) {
    }
}

struct Tracer {
    explicit Tracer(std::atomic<int> &destroyed_) : destroyed(destroyed_) {}
    ~Tracer() { destroyed.fetch_add(1); }
    std::atomic<int> &destroyed;
};

} // namespace

// ---------------------------------------------------------------------------
// Basics
// ---------------------------------------------------------------------------

TEST(Scheduler, RunsSpawnedTasksToCompletion) {
    auto scheduler = makeScheduler(withWorkers(4));
    std::atomic<int> counter{0};
    scheduler->start();
    for (int i = 0; i < 1000; ++i) {
        ASSERT_TRUE(scheduler->spawn([&] { counter.fetch_add(1); }));
    }
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(counter.load(), 1000);
    scheduler->stop();
}

TEST(Scheduler, SpawnBeforeStartRunsAfterStart) {
    auto scheduler = makeScheduler(withWorkers(2));
    std::atomic<int> counter{0};
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(scheduler->spawn([&] { counter.fetch_add(1); }));
    }
    EXPECT_EQ(counter.load(), 0);
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(counter.load(), 10);
}

TEST(Scheduler, DestructorWithoutStartReleasesUnstartedTasks) {
    std::atomic<int> destroyed{0};
    {
        auto scheduler = makeScheduler(withWorkers(1));
        auto tracer = std::make_shared<Tracer>(destroyed);
        ASSERT_TRUE(scheduler->spawn([tracer] {}));
        tracer.reset();
        EXPECT_EQ(scheduler->liveTasks(), 1u);
    }
    EXPECT_EQ(destroyed.load(), 1);
}

TEST(Scheduler, YieldInterleavesTasksOnOneWorker) {
    auto scheduler = makeScheduler(withWorkers(1));
    std::vector<std::string> log;
    auto body = [&](std::string const name) {
        return [&log, name] {
            for (int i = 0; i < 3; ++i) {
                log.push_back(name);
                this_task::yield();
            }
        };
    };
    ASSERT_TRUE(scheduler->spawn(body("a")));
    ASSERT_TRUE(scheduler->spawn(body("b")));
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    std::vector<std::string> const expected{"a", "b", "a", "b", "a", "b"};
    EXPECT_EQ(log, expected);
}

TEST(Scheduler, IsTaskReflectsContext) {
    auto scheduler = makeScheduler(withWorkers(1));
    EXPECT_FALSE(this_task::isTask());
    bool inside = false;
    ASSERT_TRUE(scheduler->spawn([&] { inside = this_task::isTask(); }));
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_TRUE(inside);
}

// ---------------------------------------------------------------------------
// park / wake
// ---------------------------------------------------------------------------

TEST(Scheduler, ParkedTaskResumesOnWakeFromAnotherThread) {
    auto scheduler = makeScheduler(withWorkers(2));
    std::atomic<bool> parked{false};
    std::atomic<bool> finished{false};
    WakeToken token;
    ASSERT_TRUE(scheduler->spawn([&] {
        token = this_task::token();
        parked.store(true);
        this_task::park();
        finished.store(true);
    }));
    scheduler->start();
    while (not parked.load()) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
    EXPECT_FALSE(finished.load());
    EXPECT_TRUE(token.wake());
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_TRUE(finished.load());
    EXPECT_FALSE(token.wake()); // stale token: task is gone
}

TEST(Scheduler, WakeBeforeParkMakesParkReturnImmediately) {
    auto scheduler = makeScheduler(withWorkers(1));
    bool returned = false;
    ASSERT_TRUE(scheduler->spawn([&] {
        WakeToken const token = this_task::token();
        token.wake(); // token stored while Running
        this_task::park();
        returned = true;
    }));
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_TRUE(returned);
}

// Two tasks hand a baton back and forth through park/wake. With 4 workers
// the wake frequently races the park; the protocol must never lose one.
TEST(Scheduler, ParkWakePingPongAcrossWorkers) {
    auto scheduler = makeScheduler(withWorkers(4));
    constexpr int kRounds = 20000;
    std::atomic<int> rounds{0};
    WakeToken tokenA;
    WakeToken tokenB;
    std::atomic<int> ready{0};

    auto player = [&](WakeToken &mine, WakeToken &other, bool const starts) {
        return [&, starts] {
            mine = this_task::token();
            ready.fetch_add(1);
            while (ready.load() < 2) {
                this_task::yield();
            }
            if (not starts) {
                this_task::park();
            }
            for (int i = 0; i < kRounds; ++i) {
                rounds.fetch_add(1);
                while (not other.wake()) {
                    this_task::yield(); // other not yet registered
                }
                if (i + 1 < kRounds or starts) {
                    this_task::park();
                }
            }
        };
    };
    ASSERT_TRUE(scheduler->spawn(player(tokenA, tokenB, true)));
    ASSERT_TRUE(scheduler->spawn(player(tokenB, tokenA, false)));
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler, std::chrono::seconds{30}));
    EXPECT_EQ(rounds.load(), 2 * kRounds);
}

TEST(Scheduler, TasksMigrateBetweenWorkers) {
    auto scheduler = makeScheduler(withWorkers(4));
    std::mutex mutex;
    std::set<std::size_t> workersSeen;
    for (int t = 0; t < 32; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            for (int i = 0; i < 200; ++i) {
                {
                    std::lock_guard<std::mutex> const lock(mutex);
                    workersSeen.insert(this_task::workerIndex());
                }
                spinFor(std::chrono::microseconds{20});
                this_task::yield();
            }
        }));
    }
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_GT(workersSeen.size(), 1u);
}

TEST(Scheduler, IdleWorkersPickUpWorkSpawnedByABusyTask) {
    auto scheduler = makeScheduler(withWorkers(4));
    std::mutex mutex;
    std::set<std::size_t> workersSeen;
    ASSERT_TRUE(scheduler->spawn([&] {
        for (int i = 0; i < 64; ++i) {
            ASSERT_TRUE(scheduler->spawn([&] {
                spinFor(std::chrono::microseconds{200});
                std::lock_guard<std::mutex> const lock(mutex);
                workersSeen.insert(this_task::workerIndex());
            }));
        }
        spinFor(std::chrono::milliseconds{5}); // stay busy so others must help
    }));
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_GT(workersSeen.size(), 1u);
}

TEST(Scheduler, PinnedTaskStaysOnItsWorker) {
    auto scheduler = makeScheduler(withWorkers(4));
    std::atomic<bool> stable{true};
    std::atomic<bool> parked{false};
    WakeToken token;
    ASSERT_TRUE(scheduler->spawn([&] {
        this_task::pinToCurrentWorker();
        std::size_t const home = this_task::workerIndex();
        for (int i = 0; i < 500; ++i) {
            spinFor(std::chrono::microseconds{5});
            this_task::yield();
            if (this_task::workerIndex() != home) {
                stable.store(false);
            }
        }
        token = this_task::token();
        parked.store(true);
        this_task::park(); // woken from a foreign thread: must come back here
        if (this_task::workerIndex() != home) {
            stable.store(false);
        }
        this_task::unpin();
    }));
    // Competing load so migration would otherwise be likely.
    for (int t = 0; t < 8; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            for (int i = 0; i < 200; ++i) {
                spinFor(std::chrono::microseconds{10});
                this_task::yield();
            }
        }));
    }
    scheduler->start();
    while (not parked.load()) {
    }
    token.wake();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_TRUE(stable.load());
}

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

TEST(Scheduler, JoinFromThreadWaitsForCompletion) {
    auto scheduler = makeScheduler(withWorkers(2));
    scheduler->start();
    int value = 0;
    JoinableSpawnResult created = scheduler->spawnJoinable([&] {
        spinFor(std::chrono::milliseconds{2});
        value = 42;
    });
    ASSERT_TRUE(created);
    created.handle.join();
    EXPECT_EQ(value, 42);
    EXPECT_FALSE(created.handle);
}

TEST(Scheduler, JoinFromTaskParksUntilCompletion) {
    auto scheduler = makeScheduler(withWorkers(2));
    int order = 0;
    int childSeen = 0;
    ASSERT_TRUE(scheduler->spawn([&] {
        JoinableSpawnResult child = scheduler->spawnJoinable([&] {
            spinFor(std::chrono::milliseconds{1});
            order = 1;
        });
        ASSERT_TRUE(child);
        child.handle.join();
        childSeen = order;
    }));
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(childSeen, 1);
}

TEST(Scheduler, JoinOnAlreadyFinishedTaskReturnsAtOnce) {
    auto scheduler = makeScheduler(withWorkers(1));
    scheduler->start();
    JoinableSpawnResult created = scheduler->spawnJoinable([] {});
    ASSERT_TRUE(created);
    while (not created.handle.isDone()) {
    }
    created.handle.join();
}

TEST(Scheduler, DroppingAJoinHandleDetaches) {
    auto scheduler = makeScheduler(withWorkers(1));
    std::atomic<bool> ran{false};
    { JoinableSpawnResult created = scheduler->spawnJoinable([&] { ran.store(true); }); }
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_TRUE(ran.load());
}

// ---------------------------------------------------------------------------
// Limits and lifecycle
// ---------------------------------------------------------------------------

TEST(Scheduler, SpawnFailsBeyondMaxTasks) {
    SchedulerOptions options = withWorkers(1);
    options.maxTasks = 4;
    auto scheduler = makeScheduler(options);
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(scheduler->spawn([] {}));
    }
    SpawnResult const fifth = scheduler->spawn([] {});
    EXPECT_FALSE(fifth);
    EXPECT_EQ(fifth.error, std::errc::resource_unavailable_try_again);
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_TRUE(scheduler->spawn([] {})); // slots recycled
    ASSERT_TRUE(waitIdle(*scheduler));
}

TEST(Scheduler, StopRequestedIsObservedByCooperativeLoops) {
    auto scheduler = makeScheduler(withWorkers(2));
    std::atomic<int> exited{0};
    for (int t = 0; t < 4; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            while (not this_task::stopRequested()) {
                this_task::yield();
            }
            exited.fetch_add(1);
        }));
    }
    scheduler->start();
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
    scheduler->stop();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(exited.load(), 4);
    EXPECT_FALSE(scheduler->spawn([] {})); // refused after stop
}

TEST(Scheduler, StopWakesParkedTasksOnce) {
    auto scheduler = makeScheduler(withWorkers(2));
    std::atomic<int> exited{0};
    for (int t = 0; t < 4; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            while (not this_task::stopRequested()) {
                this_task::park(); // nobody else ever wakes us
            }
            exited.fetch_add(1);
        }));
    }
    scheduler->start();
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
    scheduler->stop();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(exited.load(), 4);
}

TEST(Scheduler, RunOnCallingThreadReturnsAfterStop) {
    auto scheduler = makeScheduler(withWorkers(2));
    std::atomic<bool> ran{false};
    ASSERT_TRUE(scheduler->spawn([&] {
        ran.store(true);
        scheduler->stop(); // from inside a task
    }));
    std::thread runner([&] { scheduler->run(); });
    runner.join();
    EXPECT_TRUE(ran.load());
    EXPECT_EQ(scheduler->liveTasks(), 0u);
}

TEST(Scheduler, ManyTasksYieldingAndParking) {
    auto scheduler = makeScheduler(withWorkers(4));
    constexpr int kTasks = 5000;
    std::atomic<int> finished{0};
    scheduler->start();
    for (int t = 0; t < kTasks; ++t) {
        ASSERT_TRUE(scheduler->spawn([&] {
            for (int i = 0; i < 5; ++i) {
                this_task::yield();
                WakeToken const me = this_task::token();
                me.wake();
                this_task::park(); // returns at once: token pending
            }
            finished.fetch_add(1);
        }));
    }
    ASSERT_TRUE(waitIdle(*scheduler, std::chrono::seconds{30}));
    EXPECT_EQ(finished.load(), kTasks);
}

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------
#if STACKFULL_HAS_EXCEPTIONS

TEST(SchedulerExceptions, JoinRethrowsTheTaskException) {
    auto scheduler = makeScheduler(withWorkers(1));
    scheduler->start();
    JoinableSpawnResult created = scheduler->spawnJoinable([] { throw std::runtime_error("task failed"); });
    ASSERT_TRUE(created);
    EXPECT_THROW(created.handle.join(), std::runtime_error);
}

namespace {

struct RecordingSink final : ExceptionSink {
    void onUnhandledException(std::exception_ptr) noexcept override { count.fetch_add(1); }
    std::atomic<int> count{0};
};

} // namespace

TEST(SchedulerExceptions, DetachedTaskExceptionGoesToTheSink) {
    RecordingSink sink;
    SchedulerOptions options = withWorkers(1);
    options.exceptionSink = &sink;
    auto scheduler = makeScheduler(options);
    ASSERT_TRUE(scheduler->spawn([] { throw 42; }));
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(sink.count.load(), 1);
}

TEST(SchedulerExceptions, StopUnwindsTasksThatStayParked) {
    std::atomic<int> destroyed{0};
    {
        auto scheduler = makeScheduler(withWorkers(2));
        for (int t = 0; t < 8; ++t) {
            ASSERT_TRUE(scheduler->spawn([&] {
                Tracer const onStack(destroyed);
                for (;;) {
                    this_task::park(); // ignores stopRequested on purpose
                }
            }));
        }
        scheduler->start();
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
        scheduler->stop();
        ASSERT_TRUE(waitIdle(*scheduler));
    }
    EXPECT_EQ(destroyed.load(), 8);
}

TEST(SchedulerExceptions, CatchBlockSurvivesYieldAndMigration) {
    auto scheduler = makeScheduler(withWorkers(4));
    std::atomic<int> good{0};
    for (int t = 0; t < 16; ++t) {
        ASSERT_TRUE(scheduler->spawn([&, t] {
            try {
                throw t;
            } catch (int const &) {
                for (int i = 0; i < 50; ++i) {
                    this_task::yield();
                }
                try {
                    std::rethrow_exception(std::current_exception());
                } catch (int const value) {
                    if (value == t) {
                        good.fetch_add(1);
                    }
                }
            }
        }));
    }
    scheduler->start();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(good.load(), 16);
}

#endif // STACKFULL_HAS_EXCEPTIONS
