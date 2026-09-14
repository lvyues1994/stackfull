#include <stackfull/sched/Scheduler.h>

#include <stackfull/coro/Fatal.h>
#include <stackfull/sched/detail/SchedulerCore.h>
#include <stackfull/sched/detail/Worker.h>

#include <atomic>
#include <cstddef>
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
    std::size_t liveTasks() const noexcept override { return core_.liveTasks.load(std::memory_order_acquire); }

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
};

} // namespace

std::unique_ptr<Scheduler> makeScheduler(SchedulerOptions const &options) {
    return std::make_unique<SchedulerImpl>(options);
}

} // namespace sched
} // namespace stackfull
