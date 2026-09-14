// Scheduler micro benchmarks.
//
//   stackfull_sched_bench [iterations]
//
// Build twice (STACKFULL_SCHED_QUEUE=BWOS / RING) to compare run queues.

#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

using namespace stackfull::sched;

namespace {

using Clock = std::chrono::steady_clock;

double nanosPerOp(Clock::time_point const start, Clock::time_point const end, long const ops) {
    auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return static_cast<double>(ns) / static_cast<double>(ops);
}

void report(char const *const name, double const nsPerOp, char const *const note) {
    std::printf("%-52s %9.1f ns  %s\n", name, nsPerOp, note);
}

void waitIdle(Scheduler &scheduler) {
    while (scheduler.liveTasks() != 0) {
        std::this_thread::yield();
    }
}

SchedulerOptions options(std::size_t const workers) {
    SchedulerOptions o;
    o.workers = workers;
    o.taskStackSize = 64 * 1024;
    return o;
}

// One task yields `iterations` times alone on one worker: the cheap path
// that returns without switching.
void benchYieldAlone(long const iterations) {
    auto scheduler = makeScheduler(options(1));
    Clock::time_point start;
    Clock::time_point end;
    scheduler->spawn([&] {
        start = Clock::now();
        for (long i = 0; i < iterations; ++i) {
            this_task::yield();
        }
        end = Clock::now();
    });
    scheduler->start();
    waitIdle(*scheduler);
    report("yield, single task (no switch)", nanosPerOp(start, end, iterations), "per yield");
}

// Two tasks alternate through yield on one worker: each yield is one direct
// task-to-task switch plus the requeue hook.
void benchYieldPair(long const iterations) {
    auto scheduler = makeScheduler(options(1));
    Clock::time_point start;
    Clock::time_point end;
    std::atomic<int> ready{0};
    auto body = [&](bool const timer) {
        return [&, timer] {
            ready.fetch_add(1);
            while (ready.load() < 2) {
                this_task::yield();
            }
            if (timer) {
                start = Clock::now();
            }
            for (long i = 0; i < iterations; ++i) {
                this_task::yield();
            }
            if (timer) {
                end = Clock::now();
            }
        };
    };
    scheduler->spawn(body(true));
    scheduler->spawn(body(false));
    scheduler->start();
    waitIdle(*scheduler);
    report("yield, two tasks alternating (direct handoff)", nanosPerOp(start, end, iterations), "per switch");
}

// Baton passing through park/wake. Same worker: the woken task lands in the
// LIFO slot and the parking task hands off to it directly. Two workers with
// pinning: each wake crosses threads through the inbox and a futex.
void benchPingPong(char const *const name, std::size_t const workers, bool const pin, long const iterations) {
    auto scheduler = makeScheduler(options(workers));
    WakeToken tokenA;
    WakeToken tokenB;
    std::atomic<int> ready{0};
    Clock::time_point start;
    Clock::time_point end;

    auto player = [&](WakeToken &mine, WakeToken &other, bool const starter) {
        return [&, starter] {
            if (pin) {
                this_task::pinToCurrentWorker();
            }
            mine = this_task::token();
            ready.fetch_add(1);
            while (ready.load() < 2) {
                this_task::yield();
            }
            if (not starter) {
                this_task::park();
            } else {
                start = Clock::now();
            }
            for (long i = 0; i < iterations; ++i) {
                while (not other.wake()) {
                }
                if (i + 1 < iterations or starter) {
                    this_task::park();
                }
            }
            if (starter) {
                end = Clock::now();
            }
        };
    };
    if (pin) {
        // Land the two players on different workers: spawn from a thread so
        // both go through the injection queue, wake both workers.
        scheduler->start();
        std::atomic<bool> aPlaced{false};
        scheduler->spawn([&] {
            this_task::pinToCurrentWorker();
            aPlaced.store(true);
            while (ready.load() < 1) {
                this_task::yield();
            }
        });
        while (not aPlaced.load()) {
        }
        scheduler->spawn(player(tokenA, tokenB, true));
        scheduler->spawn(player(tokenB, tokenA, false));
    } else {
        scheduler->spawn(player(tokenA, tokenB, true));
        scheduler->spawn(player(tokenB, tokenA, false));
        scheduler->start();
    }
    waitIdle(*scheduler);
    report(name, nanosPerOp(start, end, iterations * 2), "per wake+park handoff");
}

// spawn + run + release from inside a task, keeping at most 64 tasks
// outstanding so stacks recycle through the pool (a burst of tens of
// thousands would measure mmap instead).
void benchSpawn(std::size_t const workers, long const count) {
    auto scheduler = makeScheduler(options(workers));
    std::atomic<long> done{0};
    Clock::time_point start;
    Clock::time_point end;
    scheduler->spawn([&] {
        start = Clock::now();
        for (long i = 0; i < count; ++i) {
            while (i - done.load(std::memory_order_relaxed) >= 64) {
                this_task::yield();
            }
            while (not scheduler->spawn([&] { done.fetch_add(1, std::memory_order_relaxed); })) {
                this_task::yield();
            }
        }
        while (done.load(std::memory_order_relaxed) < count) {
            this_task::yield();
        }
        end = Clock::now();
    });
    scheduler->start();
    waitIdle(*scheduler);
    char name[64];
    std::snprintf(name, sizeof name, "spawn+run+release, %zu workers", workers);
    report(name, nanosPerOp(start, end, count), "per task");
}

} // namespace

int main(int const argc, char **const argv) {
    long const iterations = argc > 1 ? std::atol(argv[1]) : 5'000'000L;
#if defined(STACKFULL_SCHED_QUEUE_RING)
    std::printf("run queue: RING, iterations: %ld\n", iterations);
#else
    std::printf("run queue: BWOS, iterations: %ld\n", iterations);
#endif
    benchYieldAlone(iterations);
    benchYieldPair(iterations);
    benchPingPong("park/wake ping-pong, same worker", 1, false, iterations / 5);
    benchPingPong("park/wake ping-pong, two pinned workers", 2, true, iterations / 50);
    benchSpawn(1, iterations / 5);
    benchSpawn(4, iterations / 5);
    return 0;
}
