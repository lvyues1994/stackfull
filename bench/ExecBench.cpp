// Costs of the sender/receiver bridge (stackfull::exec).
//
//   stackfull_exec_bench [iterations]

#include <stackfull/exec/execution.hpp>

#include <lexec/execution.hpp>

#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

namespace ex = lexec;
namespace exec = stackfull::exec;
using namespace stackfull::sched;

namespace {

using Clock = std::chrono::steady_clock;

void report(char const *const name, Clock::duration const elapsed, long const ops) {
    auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    std::printf("%-58s %9.1f ns\n", name, static_cast<double>(ns) / static_cast<double>(ops));
}

std::unique_ptr<Scheduler> schedulerWith(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    return makeScheduler(options);
}

// Runs `body` in a task and returns how long it took.
template <class F>
Clock::duration timedInTask(Scheduler &scheduler, F body) {
    std::atomic<bool> done{false};
    Clock::duration elapsed{};
    scheduler.spawn([&] {
        auto const start = Clock::now();
        body();
        elapsed = Clock::now() - start;
        done.store(true);
    });
    while (not done.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return elapsed;
}

} // namespace

int main(int const argc, char **const argv) {
    long const iterations = argc > 1 ? std::atol(argv[1]) : 200'000L;
    std::printf("iterations: %ld\n", iterations);

    {
        auto scheduler = schedulerWith(1);
        report("wait(just(1)) in a task (completes inline)", timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       exec::wait(ex::just(1));
                   }
               }),
               iterations);
    }
    for (std::size_t const workers : {1u, 2u}) {
        auto scheduler = schedulerWith(workers);
        exec::scheduler const sf{*scheduler};
        char name[96];
        std::snprintf(name, sizeof name, "wait(schedule(own scheduler)) in a task, %zu workers", workers);
        report(name, timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       exec::wait(ex::schedule(sf));
                   }
               }),
               iterations);
        std::snprintf(name, sizeof name, "wait(exec::spawn(fn)) in a task, %zu workers", workers);
        report(name, timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       exec::wait(exec::spawn(sf, [] { return 1; }));
                   }
               }),
               iterations);
        std::snprintf(name, sizeof name, "  native: spawnJoinable + join in a task, %zu workers", workers);
        report(name, timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       scheduler->spawnJoinable([] {}).handle.join();
                   }
               }),
               iterations);
    }
    {
        auto scheduler = schedulerWith(1);
        ex::static_thread_pool pool{1};
        report("wait(schedule(lexec pool)) in a task: there and back", timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       exec::wait(ex::schedule(pool.get_scheduler()));
                   }
               }),
               iterations);
    }
    {
        auto scheduler = schedulerWith(1);
        exec::scheduler const sf{*scheduler};
        long const rounds = iterations / 10;
        auto const start = Clock::now();
        for (long i = 0; i < rounds; ++i) {
            ex::sync_wait(exec::spawn(sf, [] { return 1; }));
        }
        report("sync_wait(exec::spawn(fn)) from a plain thread", Clock::now() - start, rounds);
        ex::static_thread_pool pool{1};
        auto const poolStart = Clock::now();
        for (long i = 0; i < rounds; ++i) {
            ex::sync_wait(ex::schedule(pool.get_scheduler()));
        }
        report("  lexec only: sync_wait(schedule(pool)) from a plain thread", Clock::now() - poolStart, rounds);
    }
    return 0;
}
