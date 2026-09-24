#include <stackfull/sched/Scheduler.h>

#include <stackfull/coro/Fatal.h>
#include <stackfull/sched/detail/SchedulerCore.h>
#include <stackfull/sched/detail/Worker.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace stackfull {
namespace sched {

namespace {

struct SchedulerImpl final : Scheduler {
    explicit SchedulerImpl(SchedulerOptions const &options) : core_(options) {
        // Running from the start. Worker 0 stays reserved when the caller
        // wants to be a worker; run() supplies it.
        launchFrom(options.callerIsWorker ? 1 : 0);
        if (options.stallThreshold.count() > 0) {
            monitor = std::thread([this] { watchForStalls(); });
        }
    }

    // Over-aligned members (queues) need more than C++14 `new` guarantees.
    // The virtual destructor makes `delete` on the base pointer find these.
    static void *operator new(std::size_t const size) {
        void *memory = nullptr;
        if (::posix_memalign(&memory, alignof(SchedulerImpl), size) != 0) {
            coro::fatal("stackfull: out of memory creating a scheduler");
        }
        return memory;
    }
    static void operator delete(void *const memory) noexcept { std::free(memory); }

    ~SchedulerImpl() override {
        stop();
        if (monitor.joinable()) {
            {
                std::lock_guard<std::mutex> const lock(monitorMutex);
                monitorStop = true;
            }
            monitorWake.notify_one();
            monitor.join();
        }
        joinThreads();
    }

    void start() override { launchFrom(0); }

    void run() override {
        {
            std::lock_guard<std::mutex> const lock(lifecycle);
            STACKFULL_CHECK(core_.options.callerIsWorker, "stackfull: run() requires SchedulerOptions::callerIsWorker");
            STACKFULL_CHECK(not worker0Taken, "stackfull: run() called twice");
            worker0Taken = true;
        }
        core_.workers[0]->run();
        joinThreads();
    }

    void stop() noexcept override {
        if (core_.stopping.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        core_.wakeAllParked();
        for (auto const &worker : core_.workers) {
            core_.unparkWorker(*worker);
        }
    }

    std::size_t workerCount() const noexcept override { return core_.workers.size(); }
    std::size_t liveTasks() const noexcept override { return core_.liveTasks(); }

protected:
    detail::SchedulerCore &core() noexcept override { return core_; }

private:
    // Starts background threads for workers [first, N) not yet launched.
    void launchFrom(std::size_t const first) {
        std::lock_guard<std::mutex> const lock(lifecycle);
        for (std::size_t i = first; i < core_.workers.size(); ++i) {
            if (launched[i]) {
                continue;
            }
            if (i == 0) {
                if (worker0Taken) {
                    continue;
                }
                worker0Taken = true;
            }
            launched[i] = true;
            detail::Worker *const worker = core_.workers[i].get();
            threads.emplace_back([worker] { worker->run(); });
        }
    }

    // A worker counts every yield, park and finish of its tasks. One that is
    // not idle and has counted none for stallThreshold is stuck in a task (or
    // a task is hogging it): report it once per episode.
    void watchForStalls() {
        using Clock = std::chrono::steady_clock;
        struct Sample {
            std::uint32_t switches = 0;
            Clock::time_point since{};
            bool reported = false;
        };
        std::chrono::milliseconds const threshold = core_.options.stallThreshold;
        std::chrono::milliseconds const period = threshold >= std::chrono::milliseconds{2}
                                                     ? threshold / 2
                                                     : std::chrono::milliseconds{1};
        std::vector<Sample> samples(core_.workers.size());
        std::unique_lock<std::mutex> lock(monitorMutex);
        while (not monitorWake.wait_for(lock, period, [this] { return monitorStop; })) {
            Clock::time_point const now = Clock::now();
            std::uint64_t const idle = core_.idleMask.load(std::memory_order_relaxed);
            for (std::size_t i = 0; i < core_.workers.size(); ++i) {
                detail::Worker const &worker = *core_.workers[i];
                std::uint32_t const switches = worker.switchCount.load(std::memory_order_relaxed) +
                                               worker.yieldTick.load(std::memory_order_relaxed);
                bool const quiet = ((idle >> i) & 1u) != 0 or not worker.running.load(std::memory_order_relaxed);
                Sample &sample = samples[i];
                if (quiet or switches != sample.switches or sample.since == Clock::time_point{}) {
                    sample = Sample{switches, now, false};
                    continue;
                }
                auto const stalled = std::chrono::duration_cast<std::chrono::milliseconds>(now - sample.since);
                if (not sample.reported and stalled >= threshold) {
                    sample.reported = true;
                    reportStall(i, stalled);
                }
            }
        }
    }

    void reportStall(std::size_t const worker, std::chrono::milliseconds const stalledFor) noexcept {
        if (core_.options.stallSink != nullptr) {
            core_.options.stallSink->onStall(worker, stalledFor);
            return;
        }
        std::fprintf(stderr, "stackfull: worker %zu has been running one task for %lld ms without a switch\n", worker,
                     static_cast<long long>(stalledFor.count()));
    }

    void joinThreads() {
        std::vector<std::thread> mine;
        {
            std::lock_guard<std::mutex> const lock(lifecycle);
            mine.swap(threads);
        }
        for (std::thread &thread : mine) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    detail::SchedulerCore core_;
    std::mutex lifecycle;
    bool worker0Taken = false;
    std::vector<bool> launched = std::vector<bool>(detail::kMaxWorkers, false);
    std::vector<std::thread> threads;

    std::thread monitor;
    std::mutex monitorMutex;
    std::condition_variable monitorWake;
    bool monitorStop = false;
};

} // namespace

std::unique_ptr<Scheduler> makeScheduler(SchedulerOptions const &options) {
    return std::make_unique<SchedulerImpl>(options);
}

} // namespace sched
} // namespace stackfull
