#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/Coroutine.h> // EntryImpl, StackReleaseGuard
#include <stackfull/coro/detail/Sanitizer.h>
#include <stackfull/coro/detail/StackLayout.h>
#include <stackfull/fcontext/Fcontext.h>
#include <stackfull/sched/SchedulerOptions.h>
#include <stackfull/sched/detail/SchedulerCore.h>
#include <stackfull/sched/detail/Task.h>
#include <stackfull/sched/detail/Worker.h>
#include <stackfull/stack/StackAllocator.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>

namespace stackfull {
namespace sched {
namespace detail {

struct TaskCreation {
    Task *task = nullptr;
    std::error_code error;
};

// Returns a slab index; false when maxTasks are alive.
inline bool acquireSlot(SchedulerCore &core, std::uint32_t &slot) noexcept {
    for (unsigned spins = 0;; ++spins) {
        queue::PopStatus const status = core.freeSlots.pop(slot);
        if (status == queue::PopStatus::Ok) {
            return true;
        }
        if (status == queue::PopStatus::Empty) {
            return false;
        }
        if (spins > 64) {
            std::this_thread::yield();
        }
    }
}

inline void releaseSlot(SchedulerCore &core, std::uint32_t const slot) noexcept {
    for (unsigned spins = 0;; ++spins) {
        if (core.freeSlots.push(slot) == queue::PushStatus::Ok) {
            return;
        }
        if (spins > 64) {
            std::this_thread::yield();
        }
    }
}

struct SlotReleaseGuard {
    SchedulerCore *core;
    std::uint32_t slot;
    ~SlotReleaseGuard() {
        if (core != nullptr) {
            releaseSlot(*core, slot);
        }
    }
    void release() noexcept { core = nullptr; }
};

// Builds a Task and its body at the top of a fresh stack. Same single
// allocation as makeCoroutine; the slab slot is the only other resource.
// The task is registered in the slab but not yet scheduled.
template <class F>
TaskCreation createTask(SchedulerCore &core, F &&body, TaskOptions const &options, JoinState *const join) {
    using Body = typename std::decay<F>::type;
    using Impl = coro::detail::EntryImpl<Body>;

    if (core.stopping.load(std::memory_order_acquire)) {
        return TaskCreation{nullptr, std::make_error_code(std::errc::operation_canceled)};
    }
    std::uint32_t slot = 0;
    if (not acquireSlot(core, slot)) {
        return TaskCreation{nullptr, std::make_error_code(std::errc::resource_unavailable_try_again)};
    }
    SlotReleaseGuard slotGuard{&core, slot};

    std::size_t const stackSize = options.stackSize != 0 ? options.stackSize : core.options.taskStackSize;
    stack::StackAllocation const allocation = core.allocator->allocate(stackSize);
    if (not allocation) {
        return TaskCreation{nullptr, allocation.error};
    }
    coro::detail::StackReleaseGuard stackGuard{core.allocator, allocation.stack};

    coro::detail::StackLayout const layout =
        coro::detail::carveStack(allocation.stack, sizeof(Impl), alignof(Impl), sizeof(Task), alignof(Task));
    if (not layout.fits) {
        return TaskCreation{nullptr, std::make_error_code(std::errc::invalid_argument)};
    }

    coro::detail::Entry *const entry = ::new (layout.entry) Impl{std::forward<F>(body)};
    auto *const task = ::new (layout.block) Task{};
    task->entry = entry;
    task->stack = allocation.stack;
    task->allocator = core.allocator;
    task->scheduler = &core;
    task->slot = slot;
    task->generation = core.slots[slot].generation.load(std::memory_order_acquire);
    task->join = join;
#if STACKFULL_HAS_ASAN
    task->asanBottom = allocation.stack.base;
    task->asanSize = allocation.stack.size;
#endif
    coro::detail::tsanCreateFiber(*task);
    task->fctx = fcontext::make(layout.stackTop, layout.usableSize, &taskEntry);

    core.slots[slot].task.store(task, std::memory_order_release);
    core.liveTasks.fetch_add(1, std::memory_order_acq_rel);
    stackGuard.release();
    slotGuard.release();
    return TaskCreation{task, std::error_code{}};
}

} // namespace detail
} // namespace sched
} // namespace stackfull
