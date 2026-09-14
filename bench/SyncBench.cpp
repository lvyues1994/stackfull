// Synchronization primitive micro benchmarks.
//
//   stackfull_sync_bench [iterations]

#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/Channel.h>
#include <stackfull/sync/Mutex.h>
#include <stackfull/sync/WaitGroup.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>

using namespace stackfull::sched;
using namespace stackfull::sync;

namespace {

using Clock = std::chrono::steady_clock;

double nanosPerOp(Clock::time_point const start, Clock::time_point const end, long const ops) {
    auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return static_cast<double>(ns) / static_cast<double>(ops);
}

void report(char const *const name, double const nsPerOp, char const *const note) {
    std::printf("%-52s %9.1f ns  %s\n", name, nsPerOp, note);
}

SchedulerOptions options(std::size_t const workers) {
    SchedulerOptions o;
    o.workers = workers;
    o.taskStackSize = 64 * 1024;
    return o;
}

void benchUncontendedMutex(long const iterations) {
    auto scheduler = makeScheduler(options(1));
    Mutex mutex;
    WaitGroup done;
    done.add(1);
    Clock::time_point start;
    Clock::time_point end;
    scheduler->spawn([&] {
        start = Clock::now();
        for (long i = 0; i < iterations; ++i) {
            mutex.lock();
            mutex.unlock();
        }
        end = Clock::now();
        done.done();
    });
    scheduler->start();
    done.wait();
    report("Mutex lock+unlock, uncontended", nanosPerOp(start, end, iterations), "per pair");
}

void benchContendedMutex(std::size_t const workers, int const tasks, long const iterations) {
    auto scheduler = makeScheduler(options(workers));
    Mutex mutex;
    WaitGroup done;
    long counter = 0;
    long const perTask = iterations / tasks;
    done.add(static_cast<std::size_t>(tasks));
    auto const start = Clock::now();
    for (int t = 0; t < tasks; ++t) {
        scheduler->spawn([&] {
            for (long i = 0; i < perTask; ++i) {
                LockGuard const guard(mutex);
                ++counter;
            }
            done.done();
        });
    }
    scheduler->start();
    done.wait();
    auto const end = Clock::now();
    char name[80];
    std::snprintf(name, sizeof name, "Mutex lock+unlock, %d tasks on %zu workers", tasks, workers);
    report(name, nanosPerOp(start, end, perTask * tasks), counter == perTask * tasks ? "per pair" : "per pair (COUNT MISMATCH)");
}

void benchChannel(std::size_t const workers, std::size_t const capacity, long const items) {
    auto scheduler = makeScheduler(options(workers));
    Channel<long> channel(capacity);
    WaitGroup done;
    done.add(2);
    long sum = 0;
    auto const start = Clock::now();
    scheduler->spawn([&] {
        for (long i = 0; i < items; ++i) {
            channel.send(i);
        }
        channel.close();
        done.done();
    });
    scheduler->spawn([&] {
        long value = 0;
        while (channel.recv(value)) {
            sum += value;
        }
        done.done();
    });
    scheduler->start();
    done.wait();
    auto const end = Clock::now();
    char name[80];
    std::snprintf(name, sizeof name, "Channel<long> cap %zu, 1P/1C on %zu workers", capacity, workers);
    report(name, nanosPerOp(start, end, items), sum == items * (items - 1) / 2 ? "per item" : "per item (SUM MISMATCH)");
}

} // namespace

int main(int const argc, char **const argv) {
    long const iterations = argc > 1 ? std::atol(argv[1]) : 2'000'000L;
    std::printf("iterations: %ld\n", iterations);
    benchUncontendedMutex(iterations);
    benchContendedMutex(1, 4, iterations);
    benchContendedMutex(4, 8, iterations);
    benchChannel(1, 64, iterations);
    benchChannel(2, 64, iterations);
    benchChannel(2, 1, iterations / 4);
    return 0;
}
