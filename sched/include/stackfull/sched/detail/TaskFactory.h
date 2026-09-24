#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/Coroutine.h> // EntryImpl, StackReleaseGuard
#include <stackfull/coro/detail/Sanitizer.h>
#include <stackfull/coro/detail/StackLayout.h>
#include <stackfull/fcontext/Fcontext.h>
#include <stackfull/sched/SchedulerOptions.h>
#include <stackfull/sched/detail/Runtime.h>
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

struct SlotReleaseGuard {
    SchedulerCore *core;
    Worker *me;
    std::uint32_t slot;
    ~SlotReleaseGuard() {
        if (core != nullptr) {
            core->releaseSlot(me, slot);
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
    Worker *const me = core.currentWorker();
    std::uint32_t slot = 0;
    if (not core.acquireSlot(me, slot)) {
        return TaskCreation{nullptr, std::make_error_code(std::errc::resource_unavailable_try_again)};
    }
    SlotReleaseGuard slotGuard{&core, me, slot};

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
    task->name = options.name;
    if (core.options.checkStackCanary) {
        task->stackCanary = static_cast<std::uint64_t *>(allocation.stack.base);
        for (std::size_t i = 0; i < kStackCanaryWords; ++i) {
            task->stackCanary[i] = kStackCanary;
        }
    }
#if STACKFULL_HAS_ASAN
    task->asanBottom = allocation.stack.base;
    task->asanSize = allocation.stack.size;
#endif
    coro::detail::tsanCreateFiber(*task);
    task->fctx = fcontext::make(layout.stackTop, layout.usableSize, &taskEntry);

    core.slots[slot].task.store(task, std::memory_order_release);
    core.countCreated(me); // before the task can run, so its finish is never counted first
    stackGuard.release();
    slotGuard.release();
    return TaskCreation{task, std::error_code{}};
}

} // namespace detail
} // namespace sched
} // namespace stackfull
