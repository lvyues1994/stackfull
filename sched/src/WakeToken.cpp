#include <stackfull/sched/WakeToken.h>

#include <stackfull/sched/detail/Runtime.h>
#include <stackfull/sched/detail/SchedulerCore.h>

#include <atomic>

namespace stackfull {
namespace sched {

bool WakeToken::wake() const noexcept {
    if (core == nullptr) {
        return false;
    }
    detail::TaskSlot &entry = core->slots[slot];
    // Pin the slot so releaseTask() cannot free the Task under us, then make
    // sure the slot still holds *our* incarnation.
    entry.pins.fetch_add(1, std::memory_order_acq_rel);
    bool woke = false;
    if (entry.generation.load(std::memory_order_acquire) == generation) {
        detail::Task *const task = entry.task.load(std::memory_order_acquire);
        if (task != nullptr) {
            core->wake(*task);
            woke = true;
        }
    }
    entry.pins.fetch_sub(1, std::memory_order_release);
    return woke;
}

} // namespace sched
} // namespace stackfull
