#include <stackfull/sync/detail/Waiter.h>

#include <stackfull/sched/Parker.h>
#include <stackfull/sched/ThisTask.h>

#include <atomic>
#include <memory>
#include <thread>

namespace stackfull {
namespace sync {
namespace detail {

namespace {

// One Parker per OS thread that ever blocks on a primitive outside a task.
sched::Parker &threadParker() {
    static thread_local std::unique_ptr<sched::Parker> parker;
    if (parker == nullptr) {
        parker = sched::makeParker();
    }
    return *parker;
}

} // namespace

Waiter::Waiter() noexcept {
    if (sched::this_task::isTask()) {
        token = sched::this_task::token();
    } else {
        parker = &threadParker();
    }
}

void Waiter::wait() {
    if (parker != nullptr) {
        while (not satisfied.load(std::memory_order_acquire)) {
            parker->park();
        }
        return;
    }
    while (not satisfied.load(std::memory_order_acquire)) {
        sched::this_task::park();
    }
}

void Waiter::awaitNotifier() noexcept {
    // The notifier is between unlinking us and storing `satisfied`: a window
    // of a few instructions on another thread.
    for (unsigned spins = 0; not satisfied.load(std::memory_order_acquire); ++spins) {
        if (spins > 64) {
            std::this_thread::yield();
        }
    }
}

} // namespace detail
} // namespace sync
} // namespace stackfull
