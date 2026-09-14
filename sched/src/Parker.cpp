#include <stackfull/sched/Parker.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>

#if defined(__linux__)
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cerrno>
#include <ctime>
#else
#include <condition_variable>
#include <mutex>
#endif

namespace stackfull {
namespace sched {

namespace {

enum : std::uint32_t { kEmpty = 0, kNotified = 1, kParked = 2 };

#if defined(__linux__)

// One 32-bit word, futex-waited on directly. Linux and Android (bionic
// exposes the raw syscall) share this path.
struct FutexParker final : Parker {
    void park() override {
        std::uint32_t expected = kEmpty;
        if (state.compare_exchange_strong(expected, kParked, std::memory_order_acq_rel)) {
            while (state.load(std::memory_order_acquire) == kParked) {
                ::syscall(SYS_futex, &state, FUTEX_WAIT_PRIVATE, kParked, nullptr, nullptr, 0);
            }
        }
        state.store(kEmpty, std::memory_order_release); // consume the token
    }

    bool parkFor(std::chrono::nanoseconds const timeout) override {
        std::uint32_t expected = kEmpty;
        bool notified = true;
        if (state.compare_exchange_strong(expected, kParked, std::memory_order_acq_rel)) {
            auto const deadline = std::chrono::steady_clock::now() + timeout;
            while (state.load(std::memory_order_acquire) == kParked) {
                auto const now = std::chrono::steady_clock::now();
                if (now >= deadline) {
                    // Give up the parked state; a racing unpark that already
                    // observed kParked will issue a harmless futex wake.
                    std::uint32_t parked = kParked;
                    if (state.compare_exchange_strong(parked, kEmpty, std::memory_order_acq_rel)) {
                        notified = false;
                    }
                    break;
                }
                auto const remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
                struct timespec ts;
                ts.tv_sec = static_cast<time_t>(remaining.count() / 1000000000LL);
                ts.tv_nsec = static_cast<long>(remaining.count() % 1000000000LL);
                ::syscall(SYS_futex, &state, FUTEX_WAIT_PRIVATE, kParked, &ts, nullptr, 0);
            }
        }
        state.store(kEmpty, std::memory_order_release);
        return notified;
    }

    void unpark() override {
        if (state.exchange(kNotified, std::memory_order_acq_rel) == kParked) {
            ::syscall(SYS_futex, &state, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
        }
    }

    std::atomic<std::uint32_t> state{kEmpty};
};

#else

struct CondvarParker final : Parker {
    void park() override {
        std::unique_lock<std::mutex> lock(mutex);
        while (not notified) {
            condition.wait(lock);
        }
        notified = false;
    }

    bool parkFor(std::chrono::nanoseconds const timeout) override {
        std::unique_lock<std::mutex> lock(mutex);
        auto const deadline = std::chrono::steady_clock::now() + timeout;
        while (not notified) {
            if (condition.wait_until(lock, deadline) == std::cv_status::timeout and not notified) {
                return false;
            }
        }
        notified = false;
        return true;
    }

    void unpark() override {
        {
            std::lock_guard<std::mutex> const lock(mutex);
            notified = true;
        }
        condition.notify_one();
    }

    std::mutex mutex;
    std::condition_variable condition;
    bool notified = false;
};

#endif

} // namespace

std::unique_ptr<Parker> makeParker() {
#if defined(__linux__)
    return std::make_unique<FutexParker>();
#else
    return std::make_unique<CondvarParker>();
#endif
}

} // namespace sched
} // namespace stackfull
