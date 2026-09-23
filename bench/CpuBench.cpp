// CPU cost of a scheduler with little or no work: no tasks, parked tasks,
// timer wakeups, and a trickle of empty tasks spawned from a foreign thread.
//
//   stackfull_cpu_bench [seconds-per-case] [case-name-substring]
//
// CPU time comes from /proc/self/task/*/schedstat (ns, per thread). The
// foreign producer thread is excluded, so the figures are the scheduler's
// own: "cores" is scheduler CPU / wall time, "us/event" divides it by the
// events in the window, "wakeups/event" counts how often a worker thread was
// scheduled onto a CPU per event.

#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/Sleep.h>
#include <stackfull/sync/WaitGroup.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

#include <dirent.h>
#include <sys/syscall.h>
#include <unistd.h>

using namespace stackfull;
using namespace stackfull::sched;
using stackfull::sync::WaitGroup;
using std::chrono::microseconds;
using std::chrono::milliseconds;

namespace {

using Clock = std::chrono::steady_clock;

struct ThreadTime {
    long tid = 0;
    std::uint64_t runNanos = 0;
    std::uint64_t slices = 0;
};

std::vector<ThreadTime> threadTimes() {
    std::vector<ThreadTime> out;
    DIR *const dir = ::opendir("/proc/self/task");
    if (dir == nullptr) {
        return out;
    }
    while (dirent const *const entry = ::readdir(dir)) {
        if (entry->d_name[0] == '.') {
            continue;
        }
        char path[sizeof "/proc/self/task//schedstat" + sizeof entry->d_name];
        std::snprintf(path, sizeof path, "/proc/self/task/%s/schedstat", entry->d_name);
        FILE *const file = std::fopen(path, "r");
        if (file == nullptr) {
            continue;
        }
        unsigned long long run = 0;
        unsigned long long wait = 0;
        unsigned long long slices = 0;
        if (std::fscanf(file, "%llu %llu %llu", &run, &wait, &slices) == 3) {
            out.push_back(ThreadTime{std::atol(entry->d_name), run, slices});
        }
        std::fclose(file);
    }
    ::closedir(dir);
    return out;
}

long currentTid() {
    return static_cast<long>(::syscall(SYS_gettid));
}

struct Window {
    Clock::time_point start;
    std::vector<ThreadTime> before;
};

Window openWindow() {
    return Window{Clock::now(), threadTimes()};
}

// Excludes the calling (main) thread and `excludedTid` (a foreign producer).
void closeWindow(char const *const name, Window const &window, double const events, long const excludedTid) {
    std::vector<ThreadTime> const after = threadTimes();
    double const wall = std::chrono::duration<double>(Clock::now() - window.start).count();
    long const self = currentTid();

    double cpu = 0;
    double busiest = 0;
    double slices = 0;
    int active = 0;
    for (ThreadTime const &now : after) {
        if (now.tid == self or now.tid == excludedTid) {
            continue;
        }
        for (ThreadTime const &then : window.before) {
            if (then.tid != now.tid) {
                continue;
            }
            double const run = static_cast<double>(now.runNanos - then.runNanos) * 1e-9;
            cpu += run;
            slices += static_cast<double>(now.slices - then.slices);
            busiest = run > busiest ? run : busiest;
            active += run > 0.01 * wall ? 1 : 0;
        }
    }

    double const rate = events / wall;
    std::printf("%-44s %7.2f%% %9.0f/s %9.2f %8.2f %5d %7.1f%%\n", name, 100.0 * cpu / wall, rate,
                events > 0 ? 1e6 * cpu / events : 0.0, events > 0 ? slices / events : 0.0, active,
                100.0 * busiest / wall);
}

void header() {
    std::printf("%-44s %8s %11s %9s %8s %5s %8s\n", "case", "cores", "events", "us/event", "wakeups", "hot",
                "busiest");
}

void waitIdle(Scheduler &scheduler) {
    while (scheduler.liveTasks() != 0) {
        std::this_thread::sleep_for(milliseconds{1});
    }
}

std::unique_ptr<Scheduler> schedulerWith(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    options.taskStackSize = 64 * 1024;
    return makeScheduler(options);
}

std::chrono::duration<double> caseLength{1.5};
char const *caseFilter = nullptr;

bool selected(char const *const name) {
    return caseFilter == nullptr or std::strstr(name, caseFilter) != nullptr;
}

// No tasks at all.
void idle(std::size_t const workers) {
    char name[64];
    std::snprintf(name, sizeof name, "idle, %zu workers", workers);
    if (not selected(name)) {
        return;
    }
    auto scheduler = schedulerWith(workers);
    std::this_thread::sleep_for(milliseconds{200});
    Window const window = openWindow();
    std::this_thread::sleep_for(caseLength);
    closeWindow(name, window, 0, 0);
}

// `count` tasks parked on a WaitGroup nobody signals during the window.
void parked(std::size_t const workers, int const count) {
    char name[64];
    std::snprintf(name, sizeof name, "%d parked tasks, %zu workers", count, workers);
    if (not selected(name)) {
        return;
    }
    auto scheduler = schedulerWith(workers);
    WaitGroup gate;
    gate.add(1);
    for (int i = 0; i < count; ++i) {
        scheduler->spawn([&] { gate.wait(); });
    }
    std::this_thread::sleep_for(milliseconds{300});
    Window const window = openWindow();
    std::this_thread::sleep_for(caseLength);
    closeWindow(name, window, 0, 0);
    gate.done();
    waitIdle(*scheduler);
}

// `tasks` tasks, each looping sleepFor(period) and doing nothing else.
void timers(std::size_t const workers, int const tasks, microseconds const period) {
    char name[64];
    std::snprintf(name, sizeof name, "%d x sleepFor(%ldus), %zu workers", tasks, static_cast<long>(period.count()),
                  workers);
    if (not selected(name)) {
        return;
    }
    auto scheduler = schedulerWith(workers);
    std::atomic<bool> stop{false};
    std::atomic<long> wakeups{0};
    for (int i = 0; i < tasks; ++i) {
        scheduler->spawn([&] {
            while (not stop.load(std::memory_order_relaxed)) {
                this_task::sleepFor(period);
                wakeups.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    std::this_thread::sleep_for(milliseconds{300});
    long const first = wakeups.load();
    Window const window = openWindow();
    std::this_thread::sleep_for(caseLength);
    closeWindow(name, window, static_cast<double>(wakeups.load() - first), 0);
    stop.store(true);
    waitIdle(*scheduler);
}

// A foreign thread spawns an empty task every `period`; below 1 ms it paces
// by spinning (its own CPU is excluded from the figures).
void trickle(std::size_t const workers, microseconds const period) {
    char name[64];
    std::snprintf(name, sizeof name, "empty task every %ldus (foreign), %zu workers",
                  static_cast<long>(period.count()), workers);
    if (not selected(name)) {
        return;
    }
    auto scheduler = schedulerWith(workers);
    std::atomic<bool> stop{false};
    std::atomic<long> spawned{0};
    std::atomic<long> producerTid{0};
    std::thread producer([&] {
        producerTid.store(currentTid());
        Clock::time_point next = Clock::now();
        while (not stop.load(std::memory_order_relaxed)) {
            next += period;
            if (period >= milliseconds{1}) {
                std::this_thread::sleep_until(next);
            } else {
                while (Clock::now() < next) {
                }
            }
            if (scheduler->spawn([] {})) {
                spawned.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
    std::this_thread::sleep_for(milliseconds{300});
    long const first = spawned.load();
    Window const window = openWindow();
    std::this_thread::sleep_for(caseLength);
    closeWindow(name, window, static_cast<double>(spawned.load() - first), producerTid.load());
    stop.store(true);
    producer.join();
    waitIdle(*scheduler);
}

// Reference floor: a bare std::thread sleeping until the next period.
void bareSleep(microseconds const period) {
    char name[64];
    std::snprintf(name, sizeof name, "bare thread sleep_until(%ldus)", static_cast<long>(period.count()));
    if (not selected(name)) {
        return;
    }
    std::atomic<bool> stop{false};
    std::atomic<long> wakeups{0};
    std::thread sleeper([&] {
        Clock::time_point next = Clock::now();
        while (not stop.load(std::memory_order_relaxed)) {
            next += period;
            std::this_thread::sleep_until(next);
            wakeups.fetch_add(1, std::memory_order_relaxed);
        }
    });
    std::this_thread::sleep_for(milliseconds{300});
    long const first = wakeups.load();
    Window const window = openWindow();
    std::this_thread::sleep_for(caseLength);
    closeWindow(name, window, static_cast<double>(wakeups.load() - first), 0);
    stop.store(true);
    sleeper.join();
}

// Reference floor: a bare std::thread woken through a condition variable by
// a foreign thread every `period` (the producer is excluded).
void bareCondvar(microseconds const period) {
    char name[64];
    std::snprintf(name, sizeof name, "bare thread condvar every %ldus (foreign)", static_cast<long>(period.count()));
    if (not selected(name)) {
        return;
    }
    std::mutex mutex;
    std::condition_variable ready;
    long pending = 0;
    bool stop = false;
    std::atomic<long> handled{0};
    std::thread consumer([&] {
        std::unique_lock<std::mutex> lock(mutex);
        for (;;) {
            ready.wait(lock, [&] { return pending != 0 or stop; });
            if (stop) {
                return;
            }
            pending = 0;
            handled.fetch_add(1, std::memory_order_relaxed);
        }
    });
    std::atomic<bool> producing{true};
    std::atomic<long> producerTid{0};
    std::thread producer([&] {
        producerTid.store(currentTid());
        Clock::time_point next = Clock::now();
        while (producing.load(std::memory_order_relaxed)) {
            next += period;
            if (period >= milliseconds{1}) {
                std::this_thread::sleep_until(next);
            } else {
                while (Clock::now() < next) {
                }
            }
            {
                std::lock_guard<std::mutex> const guard(mutex);
                ++pending;
            }
            ready.notify_one();
        }
    });
    std::this_thread::sleep_for(milliseconds{300});
    long const first = handled.load();
    Window const window = openWindow();
    std::this_thread::sleep_for(caseLength);
    closeWindow(name, window, static_cast<double>(handled.load() - first), producerTid.load());
    producing.store(false);
    producer.join();
    {
        std::lock_guard<std::mutex> const guard(mutex);
        stop = true;
    }
    ready.notify_one();
    consumer.join();
}

} // namespace

int main(int const argc, char **const argv) {
    if (argc > 1) {
        caseLength = std::chrono::duration<double>(std::atof(argv[1]));
    }
    if (argc > 2) {
        caseFilter = argv[2];
    }
    std::size_t const all = std::thread::hardware_concurrency();
    std::printf("hardware_concurrency: %zu, %.1f s per case\n", all, caseLength.count());
    if (threadTimes().empty()) {
        std::printf("/proc/self/task/*/schedstat unavailable\n");
        return 1;
    }
    header();

    bareSleep(microseconds{10000});
    bareSleep(microseconds{1000});
    bareCondvar(microseconds{10000});
    bareCondvar(microseconds{1000});
    bareCondvar(microseconds{100});
    bareCondvar(microseconds{10});

    idle(1);
    idle(4);
    idle(all);
    parked(all, 10000);

    for (std::size_t const workers : {std::size_t{4}, all}) {
        timers(workers, 1, microseconds{10000});
        timers(workers, 1, microseconds{1000});
        timers(workers, 100, microseconds{10000});
        timers(workers, 1000, microseconds{100000});
    }

    for (std::size_t const workers : {std::size_t{1}, std::size_t{4}, all}) {
        trickle(workers, microseconds{10000});
        trickle(workers, microseconds{1000});
        trickle(workers, microseconds{100});
        trickle(workers, microseconds{10});
    }
    return 0;
}
