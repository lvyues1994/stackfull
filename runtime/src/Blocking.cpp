#include <stackfull/runtime/Runtime.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include <pthread.h>

namespace stackfull {
namespace detail {

namespace {

constexpr std::size_t kMaxThreads = 64;
constexpr std::chrono::seconds kIdleExit{10};

struct BlockingPool {

    // More pending jobs than idle threads: add a thread (up to the cap);
    // otherwise an idle one takes it.
    void submit(std::unique_ptr<BlockingJob> job) {
        std::lock_guard<std::mutex> const lock(mutex);
        jobs.push_back(std::move(job));
        if (jobs.size() > idle and threads < kMaxThreads) {
            ++threads;
            std::thread([this] { work(); }).detach();
        } else {
            ready.notify_one();
        }
    }

    void work() {
#if defined(__linux__) || defined(__QNX__)
        ::pthread_setname_np(::pthread_self(), "sf-blocking");
#endif
        std::unique_lock<std::mutex> lock(mutex);
        for (;;) {
            if (jobs.empty()) {
                ++idle;
                bool const got = ready.wait_for(lock, kIdleExit, [this] { return not jobs.empty(); });
                --idle;
                if (not got) {
                    --threads;
                    return;
                }
            }
            std::unique_ptr<BlockingJob> job = std::move(jobs.front());
            jobs.pop_front();
            lock.unlock();
            job->run();
            job.reset();
            lock.lock();
        }
    }

    std::mutex mutex;
    std::condition_variable ready;
    std::deque<std::unique_ptr<BlockingJob>> jobs;
    std::size_t threads = 0;
    std::size_t idle = 0;
};

BlockingPool &blockingPool() {
    static BlockingPool *const pool = new BlockingPool; // immortal: detached threads use it
    return *pool;
}

} // namespace

void submitBlocking(std::unique_ptr<BlockingJob> job) {
    blockingPool().submit(std::move(job));
}

} // namespace detail
} // namespace stackfull
