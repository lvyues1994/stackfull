// Multicore scaling, per-task footprint and wake latency.
//
//   stackfull_scale_bench [scale|yield|spawn|pingpong|mem|lat|all]
//
// bench/tokio-compare runs the same cases on Tokio for reference.

#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sched/WakeToken.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace stackfull::sched;
using std::chrono::microseconds;
using std::chrono::milliseconds;

namespace {

using Clock = std::chrono::steady_clock;

constexpr milliseconds kWarmup{50};
constexpr milliseconds kWindow{500};

std::unique_ptr<Scheduler> schedulerWith(std::size_t const workers, std::size_t const stackSize = 64 * 1024,
                                         std::size_t const maxTasks = 65536) {
    SchedulerOptions options;
    options.workers = workers;
    options.taskStackSize = stackSize;
    options.maxTasks = maxTasks;
    return makeScheduler(options);
}

void waitIdle(Scheduler &scheduler) {
    while (scheduler.liveTasks() != 0) {
        std::this_thread::sleep_for(milliseconds{1});
    }
}

struct alignas(64) Counter {
    std::atomic<long> value{0};
};

// Sleeps for the measurement window, then raises `stop`; returns the window in seconds.
double measureWindow(std::atomic<bool> &stop) {
    auto const start = Clock::now();
    std::this_thread::sleep_for(kWindow);
    stop.store(true);
    return std::chrono::duration<double>(Clock::now() - start).count();
}

// 4 tasks per worker, each yielding in a loop.
void scaleYield(std::size_t const workers) {
    auto scheduler = schedulerWith(workers);
    std::atomic<bool> stop{false};
    std::atomic<bool> counting{false};
    std::vector<Counter> counts(4 * workers);
    for (Counter &count : counts) {
        Counter *const slot = &count;
        scheduler->spawn([slot, &stop, &counting] {
            long yields = 0;
            while (not stop.load(std::memory_order_relaxed)) {
                this_task::yield();
                yields += counting.load(std::memory_order_relaxed) ? 1 : 0;
            }
            slot->value.store(yields);
        });
    }
    std::this_thread::sleep_for(kWarmup);
    counting.store(true);
    double const seconds = measureWindow(stop);
    waitIdle(*scheduler);
    long total = 0;
    for (Counter const &count : counts) {
        total += count.value.load();
    }
    double const rate = static_cast<double>(total) / seconds;
    std::printf("yield        %2zu workers  %8.1f M/s  (%.1f M/s per worker)\n", workers, rate / 1e6,
                rate / 1e6 / static_cast<double>(workers));
}

// Four root tasks per worker spawn empty children, at most 16 outstanding
// each. Several roots per worker keep every worker busy: an idle one would
// have the others feed it through the injection queue instead.
void scaleSpawn(std::size_t const workers) {
    auto scheduler = schedulerWith(workers);
    std::atomic<bool> stop{false};
    std::vector<Counter> done(4 * workers);
    for (Counter &rootDone : done) {
        Counter *const finished = &rootDone;
        scheduler->spawn([&scheduler, &stop, finished] {
            long issued = 0;
            while (not stop.load(std::memory_order_relaxed)) {
                while (issued - finished->value.load(std::memory_order_relaxed) >= 16) {
                    this_task::yield();
                }
                if (scheduler->spawn([finished] { finished->value.fetch_add(1, std::memory_order_relaxed); })) {
                    ++issued;
                } else {
                    this_task::yield();
                }
            }
        });
    }
    std::this_thread::sleep_for(kWarmup);
    long before = 0;
    for (Counter const &rootDone : done) {
        before += rootDone.value.load();
    }
    auto const start = Clock::now();
    std::this_thread::sleep_for(kWindow);
    long after = 0;
    for (Counter const &rootDone : done) {
        after += rootDone.value.load();
    }
    double const seconds = std::chrono::duration<double>(Clock::now() - start).count();
    stop.store(true);
    waitIdle(*scheduler);
    double const tasks = static_cast<double>(after - before);
    std::printf("spawn fanout %2zu workers  %8.2f M/s  (%.0f ns per task per worker)\n", workers, tasks / seconds / 1e6,
                1e9 * seconds * static_cast<double>(workers) / tasks);
}

struct PingPongPair {
    std::atomic<int> turn{0};
    std::atomic<int> ready{0};
    WakeToken sides[2];
    long trips = 0;
};

// Two pairs per worker, each side waking the other and parking.
void scalePingPong(std::size_t const workers) {
    auto scheduler = schedulerWith(workers);
    std::atomic<bool> stop{false};
    std::vector<std::unique_ptr<PingPongPair>> pairs;
    for (std::size_t i = 0; i < 2 * workers; ++i) {
        pairs.push_back(std::make_unique<PingPongPair>());
    }
    for (auto const &owned : pairs) {
        PingPongPair *const pair = owned.get();
        for (int me = 0; me < 2; ++me) {
            scheduler->spawn([pair, me, &stop] {
                pair->sides[me] = this_task::token();
                pair->ready.fetch_add(1);
                while (pair->ready.load() < 2) {
                    this_task::yield();
                }
                WakeToken const other = pair->sides[1 - me];
                long trips = 0;
                for (;;) {
                    while (pair->turn.load(std::memory_order_acquire) != me) {
                        if (stop.load(std::memory_order_relaxed)) {
                            other.wake();
                            if (me == 0) {
                                pair->trips = trips;
                            }
                            return;
                        }
                        this_task::park();
                    }
                    pair->turn.store(1 - me, std::memory_order_release);
                    other.wake();
                    ++trips;
                }
            });
        }
    }
    std::this_thread::sleep_for(kWarmup);
    double const seconds = measureWindow(stop);
    waitIdle(*scheduler);
    long total = 0;
    for (auto const &pair : pairs) {
        total += pair->trips;
    }
    std::printf("park/wake    %2zu workers  %8.2f M handoffs/s over %zu pairs\n", workers,
                static_cast<double>(total) / seconds / 1e6, pairs.size());
}

long statusKiB(char const *const field) {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind(field, 0) == 0) {
            return std::atol(line.c_str() + std::string(field).size());
        }
    }
    return 0;
}

long mappingCount() {
    std::ifstream maps("/proc/self/maps");
    std::string line;
    long count = 0;
    while (std::getline(maps, line)) {
        ++count;
    }
    return count;
}

// `count` parked tasks: spawn cost from a cold stack pool, resident memory and
// memory mappings per task.
void footprint(int const count, std::size_t const stackSize) {
    auto scheduler = schedulerWith(4, stackSize, 126976);
    std::this_thread::sleep_for(kWarmup);
    long const rssBefore = statusKiB("VmRSS:");
    long const mapsBefore = mappingCount();
    std::atomic<int> parked{0};
    std::atomic<bool> release{false};
    auto const start = Clock::now();
    int spawned = 0;
    for (; spawned < count; ++spawned) {
        bool const ok = static_cast<bool>(scheduler->spawn([&] {
            parked.fetch_add(1);
            while (not release.load()) {
                this_task::park();
            }
        }));
        if (not ok) {
            break;
        }
    }
    double const spawnSeconds = std::chrono::duration<double>(Clock::now() - start).count();
    while (parked.load() < spawned) {
        std::this_thread::sleep_for(milliseconds{1});
    }
    double const per = static_cast<double>(spawned);
    std::printf("footprint    %6d tasks x %3zu KiB stack: spawned %d, %.0f ns/spawn, RSS +%.1f KiB/task, "
                "mappings +%.2f/task\n",
                count, stackSize / 1024, spawned, 1e9 * spawnSeconds / per,
                static_cast<double>(statusKiB("VmRSS:") - rssBefore) / per,
                static_cast<double>(mappingCount() - mapsBefore) / per);
    release.store(true);
    scheduler->stop();
    waitIdle(*scheduler);
}

void reportLatency(char const *const name, std::vector<double> samples) {
    std::sort(samples.begin(), samples.end());
    auto const at = [&samples](double const q) {
        auto const index = static_cast<std::size_t>(q * static_cast<double>(samples.size()));
        return samples[std::min(samples.size() - 1, index)];
    };
    std::printf("%-44s p50 %7.1f  p99 %7.1f  p99.9 %7.1f  max %8.1f us\n", name, at(0.5), at(0.99), at(0.999),
                samples.back());
}

long nowNanos() {
    return static_cast<long>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

// Paces `period` apart: sleeps for millisecond periods, spins below.
void paceUntil(Clock::time_point const next, microseconds const period) {
    if (period >= milliseconds{1}) {
        std::this_thread::sleep_until(next);
    } else {
        while (Clock::now() < next) {
        }
    }
}

// A foreign thread wakes a parked task; latency from wake() to the task running.
void wakeLatency(std::size_t const workers, microseconds const period, int const samples) {
    auto scheduler = schedulerWith(workers);
    std::atomic<bool> haveToken{false};
    WakeToken token{};
    std::atomic<long> sentAt{0};
    std::atomic<int> sequence{0};
    std::atomic<int> received{0};
    std::vector<double> latencies;
    latencies.reserve(static_cast<std::size_t>(samples));
    scheduler->spawn([&] {
        token = this_task::token();
        haveToken.store(true);
        for (int seen = 0; seen < samples;) {
            while (sequence.load(std::memory_order_acquire) == seen) {
                this_task::park();
            }
            latencies.push_back(static_cast<double>(nowNanos() - sentAt.load()) / 1000.0);
            ++seen;
            received.store(seen, std::memory_order_release);
        }
    });
    while (not haveToken.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(milliseconds{20});
    Clock::time_point next = Clock::now();
    for (int i = 0; i < samples; ++i) {
        next += period;
        paceUntil(next, period);
        sentAt.store(nowNanos());
        sequence.store(i + 1, std::memory_order_release);
        token.wake();
        while (received.load(std::memory_order_acquire) <= i) {
            if (period >= milliseconds{1}) {
                std::this_thread::yield();
            }
        }
    }
    waitIdle(*scheduler);
    char name[96];
    std::snprintf(name, sizeof name, "wake task from thread, every %ldus, %zuw", static_cast<long>(period.count()),
                  workers);
    reportLatency(name, std::move(latencies));
}

// Reference: the same wakeups delivered to a bare thread through a condvar.
void condvarLatency(microseconds const period, int const samples) {
    std::mutex mutex;
    std::condition_variable ready;
    int sequence = 0;
    long sentAt = 0;
    std::atomic<int> received{0};
    std::vector<double> latencies;
    latencies.reserve(static_cast<std::size_t>(samples));
    std::thread consumer([&] {
        std::unique_lock<std::mutex> lock(mutex);
        for (int seen = 0; seen < samples; ++seen) {
            ready.wait(lock, [&] { return sequence > seen; });
            latencies.push_back(static_cast<double>(nowNanos() - sentAt) / 1000.0);
            received.store(seen + 1, std::memory_order_release);
        }
    });
    std::this_thread::sleep_for(milliseconds{20});
    Clock::time_point next = Clock::now();
    for (int i = 0; i < samples; ++i) {
        next += period;
        paceUntil(next, period);
        {
            std::lock_guard<std::mutex> const guard(mutex);
            sentAt = nowNanos();
            sequence = i + 1;
        }
        ready.notify_one();
        while (received.load(std::memory_order_acquire) <= i) {
            if (period >= milliseconds{1}) {
                std::this_thread::yield();
            }
        }
    }
    consumer.join();
    char name[96];
    std::snprintf(name, sizeof name, "bare thread condvar, every %ldus", static_cast<long>(period.count()));
    reportLatency(name, std::move(latencies));
}

} // namespace

int main(int const argc, char **const argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    std::string const what = argc > 1 ? argv[1] : "all";
    std::size_t const counts[] = {1, 2, 4, 8, 16, 24};
    bool const scale = what == "all" or what == "scale";
    if (scale or what == "yield") {
        for (std::size_t const workers : counts) {
            scaleYield(workers);
        }
    }
    if (scale or what == "spawn") {
        for (std::size_t const workers : counts) {
            scaleSpawn(workers);
        }
    }
    if (scale or what == "pingpong") {
        for (std::size_t const workers : counts) {
            scalePingPong(workers);
        }
    }
    if (what == "all" or what == "mem") {
        footprint(10000, 64 * 1024);
        footprint(100000, 64 * 1024);
        footprint(10000, 128 * 1024);
        footprint(100000, 16 * 1024);
    }
    if (what == "all" or what == "lat") {
        condvarLatency(microseconds{1000}, 3000);
        wakeLatency(4, microseconds{1000}, 3000);
        wakeLatency(24, microseconds{1000}, 3000);
        condvarLatency(microseconds{20}, 20000);
        wakeLatency(4, microseconds{20}, 20000);
    }
    return 0;
}
