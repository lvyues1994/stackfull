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
    explicit SchedulerImpl(SchedulerOptions const &options) : core_(options) {}

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

    void start() override {
        std::lock_guard<std::mutex> const lock(lifecycle);
        STACKFULL_CHECK(not started, "stackfull: Scheduler started twice");
        started = true;
        for (auto const &worker : core_.workers) {
            detail::Worker *const raw = worker.get();
            threads.emplace_back([raw] { raw->run(); });
        }
    }

    void run() override {
        {
            std::lock_guard<std::mutex> const lock(lifecycle);
            STACKFULL_CHECK(not started, "stackfull: Scheduler started twice");
            started = true;
            for (std::size_t i = 1; i < core_.workers.size(); ++i) {
                detail::Worker *const worker = core_.workers[i].get();
                threads.emplace_back([worker] { worker->run(); });
            }
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
            worker->parker->unpark();
        }
    }

    std::size_t workerCount() const noexcept override { return core_.workers.size(); }
    std::size_t liveTasks() const noexcept override { return core_.liveTasks.load(std::memory_order_acquire); }

protected:
    detail::SchedulerCore &core() noexcept override { return core_; }

private:
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
    bool started = false;
    std::vector<std::thread> threads;
};

} // namespace

std::unique_ptr<Scheduler> makeScheduler(SchedulerOptions const &options) {
    return std::make_unique<SchedulerImpl>(options);
}

} // namespace sched
} // namespace stackfull
