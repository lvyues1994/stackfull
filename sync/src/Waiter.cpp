#include <stackfull/sync/detail/Waiter.h>

#include <stackfull/sched/Parker.h>
#include <stackfull/sched/ThisTask.h>

#include <atomic>
#include <memory>

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

void Waiter::wait() noexcept {
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

} // namespace detail
} // namespace sync
} // namespace stackfull
