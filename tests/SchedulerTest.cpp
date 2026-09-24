#include <stackfull/sched/Affinity.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/stack/MmapStackAllocator.h>
#include <stackfull/stack/PooledStackAllocator.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <sys/mman.h>
#if defined(__linux__)
#include <sched.h>
#endif

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#include <stdexcept>
#endif

using namespace stackfull::sched;
namespace stack = stackfull::stack;

namespace {

SchedulerOptions withWorkers(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    return options;
}

// A scheduler whose single worker does not run until start()/run(): lets a
// test set up several tasks before any of them moves.
SchedulerOptions deferredSingleWorker() {
    SchedulerOptions options;
    options.workers = 1;
    options.callerIsWorker = true;
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

TEST(Scheduler, MakeSchedulerIsAlreadyRunning) {
    auto scheduler = makeScheduler(withWorkers(2));
    std::atomic<int> counter{0};
    ASSERT_TRUE(scheduler->spawn([&] { counter.fetch_add(1); }));
    ASSERT_TRUE(waitIdle(*scheduler)); // no start() needed
    EXPECT_EQ(counter.load(), 1);
}

TEST(Scheduler, ReservedWorkerRunsNothingUntilStarted) {
    auto scheduler = makeScheduler(deferredSingleWorker());
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
        auto scheduler = makeScheduler(deferredSingleWorker());
        auto tracer = std::make_shared<Tracer>(destroyed);
        ASSERT_TRUE(scheduler->spawn([tracer] {}));
        tracer.reset();
        EXPECT_EQ(scheduler->liveTasks(), 1u);
    }
    EXPECT_EQ(destroyed.load(), 1);
}

TEST(Scheduler, YieldInterleavesTasksOnOneWorker) {
    auto scheduler = makeScheduler(deferredSingleWorker());
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

// Several wakers keep waking tasks that keep parking. A task often parks
// just as a wake arrives; the park hook must then reschedule it without
// Parked ever becoming visible, or a second wake in that instant schedules
// it again and it is resumed twice (a crash).
TEST(Scheduler, ConcurrentWakesScheduleAParkingTaskOnce) {
    auto scheduler = makeScheduler(withWorkers(16));
    constexpr int kSleepers = 32;
    constexpr int kWakers = 16;
    std::vector<WakeToken> tokens(kSleepers);
    std::atomic<int> registered{0};
    std::atomic<bool> stop{false};
    std::atomic<long> resumes{0};
    for (int i = 0; i < kSleepers; ++i) {
        ASSERT_TRUE(scheduler->spawn([&, i] {
            tokens[static_cast<std::size_t>(i)] = this_task::token();
            registered.fetch_add(1);
            while (not stop.load(std::memory_order_relaxed)) {
                this_task::park(); // woken spuriously as often as not
                resumes.fetch_add(1, std::memory_order_relaxed);
            }
        }));
    }
    for (int w = 0; w < kWakers; ++w) {
        ASSERT_TRUE(scheduler->spawn([&, w] {
            while (registered.load() < kSleepers) {
                this_task::yield();
            }
            for (int round = 0; not stop.load(std::memory_order_relaxed); ++round) {
                tokens[static_cast<std::size_t>((w + round) % kSleepers)].wake();
                if ((round & 63) == 0) {
                    this_task::yield();
                }
            }
        }));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    stop.store(true);
    for (WakeToken const &token : tokens) {
        token.wake(); // release sleepers parked after their last check
    }
    ASSERT_TRUE(waitIdle(*scheduler, std::chrono::seconds{30}));
    EXPECT_GT(resumes.load(), 0);
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

// Workers keep free slots in local caches; spawn() must still succeed until
// every one of maxTasks slots is in use, wherever the free ones sit.
TEST(Scheduler, SpawnLimitIsExactWhileWorkersCacheSlots) {
    SchedulerOptions options = withWorkers(4);
    options.maxTasks = 100;
    auto scheduler = makeScheduler(options);
    constexpr int kSpawners = 4;
    std::atomic<int> children{0};
    std::atomic<int> spawnersDone{0};
    std::atomic<bool> release{false};
    for (int s = 0; s < kSpawners; ++s) {
        ASSERT_TRUE(scheduler->spawn([&] {
            while (scheduler->spawn([&] {
                while (not release.load()) {
                    this_task::park();
                }
            })) {
                children.fetch_add(1);
                this_task::yield(); // let the other spawners interleave
            }
            spawnersDone.fetch_add(1);
            while (not release.load()) {
                this_task::yield();
            }
        }));
    }
    while (spawnersDone.load() < kSpawners) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    EXPECT_EQ(children.load(), 100 - kSpawners);
    release.store(true);
    scheduler->stop(); // wakes the parked children once; they see `release`
    ASSERT_TRUE(waitIdle(*scheduler));
}

namespace {

struct CountingStackAllocator final : stack::StackAllocator {
    stack::StackAllocation allocate(std::size_t const size) noexcept override {
        allocations.fetch_add(1);
        return upstream->allocate(size);
    }
    void deallocate(stack::StackView const &view) noexcept override { upstream->deallocate(view); }

    std::unique_ptr<stack::StackAllocator> upstream = stack::makeMmapStackAllocator();
    std::atomic<int> allocations{0};
};

// Stacks with writable memory below them: an overflow corrupts memory
// silently, as it would without a guard page.
struct UnguardedStackAllocator final : stack::StackAllocator {
    static constexpr std::size_t kSlack = 256 * 1024;
    stack::StackAllocation allocate(std::size_t const size) noexcept override {
        void *const mapping = ::mmap(nullptr, size + kSlack, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (mapping == MAP_FAILED) {
            return stack::StackAllocation{stack::StackView{}, std::make_error_code(std::errc::not_enough_memory)};
        }
        return stack::StackAllocation{stack::StackView{static_cast<char *>(mapping) + kSlack, size}, std::error_code{}};
    }
    void deallocate(stack::StackView const &view) noexcept override {
        ::munmap(static_cast<char *>(view.base) - kSlack, view.size + kSlack);
    }
};

} // namespace

TEST(Scheduler, ReservedStacksServeABurstOfSpawns) {
    CountingStackAllocator upstream;
    auto const pooled = stack::makePooledStackAllocator(upstream);
    SchedulerOptions options = withWorkers(4);
    options.allocator = pooled.get();
    options.taskStackSize = 32 * 1024;
    options.reserveStacks = 500;
    auto scheduler = makeScheduler(options);
    EXPECT_EQ(upstream.allocations.load(), 500);
    std::atomic<int> parked{0};
    std::atomic<bool> release{false};
    for (int i = 0; i < 500; ++i) {
        ASSERT_TRUE(scheduler->spawn([&] {
            parked.fetch_add(1);
            while (not release.load()) {
                this_task::park();
            }
        }));
    }
    while (parked.load() < 500) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    EXPECT_EQ(upstream.allocations.load(), 500); // the burst mapped nothing
    release.store(true);
    scheduler->stop();
    ASSERT_TRUE(waitIdle(*scheduler));
}

TEST(Scheduler, StackCanaryStaysQuietForWellBehavedTasks) {
    SchedulerOptions options = withWorkers(2);
    options.checkStackCanary = true;
    auto scheduler = makeScheduler(options);
    std::atomic<int> done{0};
    for (int i = 0; i < 200; ++i) {
        ASSERT_TRUE(scheduler->spawn([&] {
            for (int k = 0; k < 10; ++k) {
                this_task::yield();
            }
            done.fetch_add(1);
        }));
    }
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(done.load(), 200);
}

TEST(SchedulerDeath, StackCanaryCatchesAnOverflowWithoutGuardPage) {
    struct Deep {
        // Fills each frame, as a real overflow (a used local buffer, deep
        // recursion) writes its memory.
        static int recurse(int const n, int const limit) {
            char pad[512];
            std::memset(pad, n, sizeof pad);
            asm volatile("" : : "r"(pad) : "memory");
            return n >= limit ? pad[0] : recurse(n + 1, limit) + pad[1];
        }
    };
    EXPECT_DEATH_IF_SUPPORTED(
        {
            UnguardedStackAllocator allocator;
            SchedulerOptions options = withWorkers(1);
            options.allocator = &allocator;
            options.taskStackSize = 32 * 1024;
            options.checkStackCanary = true;
            auto scheduler = makeScheduler(options);
            scheduler->spawn([] { Deep::recurse(0, 128); }); // ~70 KiB deep on a 32 KiB stack
            for (int i = 0; i < 2000; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
            }
        },
        "stack overflow");
}

TEST(Scheduler, WorkerStartHookRunsOnEveryWorker) {
    std::mutex mutex;
    std::set<std::size_t> started;
    SchedulerOptions options = withWorkers(3);
    options.onWorkerStart = [&](std::size_t const index) {
        std::lock_guard<std::mutex> const lock(mutex);
        started.insert(index);
    };
    auto scheduler = makeScheduler(options);
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    for (;;) {
        {
            std::lock_guard<std::mutex> const lock(mutex);
            if (started.size() == 3 or std::chrono::steady_clock::now() > deadline) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    std::lock_guard<std::mutex> const lock(mutex);
    EXPECT_EQ(started, (std::set<std::size_t>{0, 1, 2}));
}

#if defined(__linux__)
TEST(Scheduler, WorkersPinnedFromTheStartHookRunThere) {
    SchedulerOptions options = withWorkers(2);
    std::atomic<int> pinErrors{0};
    options.onWorkerStart = [&](std::size_t) {
        if (pinCurrentThreadToCpus({0})) {
            pinErrors.fetch_add(1);
        }
    };
    auto scheduler = makeScheduler(options);
    std::atomic<int> otherCpu{0};
    for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(scheduler->spawn([&] {
            if (::sched_getcpu() != 0) {
                otherCpu.fetch_add(1);
            }
        }));
    }
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(pinErrors.load(), 0);
    EXPECT_EQ(otherCpu.load(), 0);
}
#endif

namespace {

struct RecordingStallSink final : StallSink {
    void onStall(std::size_t const worker, std::chrono::milliseconds const stalledFor) noexcept override {
        std::lock_guard<std::mutex> const lock(mutex);
        reports.emplace_back(worker, stalledFor);
    }
    std::size_t count() {
        std::lock_guard<std::mutex> const lock(mutex);
        return reports.size();
    }
    std::mutex mutex;
    std::vector<std::pair<std::size_t, std::chrono::milliseconds>> reports;
};

} // namespace

TEST(Scheduler, StallMonitorReportsATaskThatNeverYields) {
    RecordingStallSink sink;
    SchedulerOptions options = withWorkers(2);
    options.stallThreshold = std::chrono::milliseconds{30};
    options.stallSink = &sink;
    auto scheduler = makeScheduler(options);
    ASSERT_TRUE(scheduler->spawn([] {
        auto const until = std::chrono::steady_clock::now() + std::chrono::milliseconds{150};
        while (std::chrono::steady_clock::now() < until) {
        }
    }));
    ASSERT_TRUE(waitIdle(*scheduler));
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    ASSERT_EQ(sink.count(), 1u); // once per episode
    EXPECT_GE(sink.reports[0].second.count(), 30);
}

TEST(Scheduler, StallMonitorIgnoresTasksThatYield) {
    RecordingStallSink sink;
    SchedulerOptions options = withWorkers(2);
    options.stallThreshold = std::chrono::milliseconds{30};
    options.stallSink = &sink;
    auto scheduler = makeScheduler(options);
    for (int t = 0; t < 2; ++t) {
        ASSERT_TRUE(scheduler->spawn([] {
            auto const until = std::chrono::steady_clock::now() + std::chrono::milliseconds{150};
            while (std::chrono::steady_clock::now() < until) {
                auto const slice = std::chrono::steady_clock::now() + std::chrono::milliseconds{1};
                while (std::chrono::steady_clock::now() < slice) {
                }
                this_task::yield();
            }
        }));
    }
    ASSERT_TRUE(waitIdle(*scheduler));
    std::this_thread::sleep_for(std::chrono::milliseconds{50}); // idle workers are not stalls either
    EXPECT_EQ(sink.count(), 0u);
}

TEST(Scheduler, SpawnFailsBeyondMaxTasks) {
    SchedulerOptions options = deferredSingleWorker(); // tasks stay alive until start()
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
    SchedulerOptions options = withWorkers(2);
    options.callerIsWorker = true;
    auto scheduler = makeScheduler(options);
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
