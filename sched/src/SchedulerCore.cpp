#include <stackfull/sched/detail/SchedulerCore.h>

#include "Unwind.h"

#include <stackfull/coro/Fatal.h>
#include <stackfull/sched/detail/JoinState.h>
#include <stackfull/sched/detail/Runtime.h>
#include <stackfull/sched/detail/TaskFactory.h>
#include <stackfull/sched/detail/Worker.h>
#include <stackfull/stack/DefaultStackAllocator.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>

namespace stackfull {
namespace sched {
namespace detail {

namespace {

std::size_t resolveWorkerCount(std::size_t const requested) noexcept {
    std::size_t count = requested != 0 ? requested : std::thread::hardware_concurrency();
    if (count == 0) {
        count = 1;
    }
    return count > kMaxWorkers ? kMaxWorkers : count;
}

#if STACKFULL_HAS_EXCEPTIONS
struct AbortingExceptionSink final : ExceptionSink {
    void onUnhandledException(std::exception_ptr) noexcept override {
        coro::fatal("stackfull: unhandled exception escaped a detached task");
    }
};

ExceptionSink &abortingExceptionSink() noexcept {
    static AbortingExceptionSink sink;
    return sink;
}
#endif

void finishJoin(JoinState &join) noexcept {
    // seq_cst on both sides makes this a Dekker pair with JoinHandle::join():
    // either we see the registered waiter or the joiner sees `done`.
    join.done.store(true, std::memory_order_seq_cst);
    std::uint8_t const kind = join.waiterKind.load(std::memory_order_seq_cst);
    if (kind == JoinState::kTaskWaiter) {
        join.taskWaiter.wake();
    } else if (kind == JoinState::kThreadWaiter) {
        join.threadWaiter->unpark();
    }
    join.release();
}

} // namespace

SchedulerCore::SchedulerCore(SchedulerOptions const &options_) : options(options_) {
    options.workers = resolveWorkerCount(options.workers);
    STACKFULL_CHECK(options.maxTasks >= 1 and options.maxTasks <= SlotQueue::kCapacity,
                    "stackfull: SchedulerOptions::maxTasks out of range for the injection queue");
    allocator = options.allocator != nullptr ? options.allocator : &stack::defaultStackAllocator();
#if STACKFULL_HAS_EXCEPTIONS
    exceptionSink = options.exceptionSink != nullptr ? options.exceptionSink : &abortingExceptionSink();
#endif

    slots = std::make_unique<TaskSlot[]>(options.maxTasks);
    for (std::uint32_t i = 0; i < options.maxTasks; ++i) {
        releaseSlot(*this, i);
    }

    workers.reserve(options.workers);
    for (std::size_t i = 0; i < options.workers; ++i) {
        workers.push_back(std::make_unique<Worker>(*this, i));
    }
}

SchedulerCore::~SchedulerCore() {
    // Tasks that were spawned but never ran (no worker ever started) still
    // own their stacks; release them here. Everything else is gone once the
    // workers have exited.
    for (std::uint32_t i = 0; i < options.maxTasks; ++i) {
        Task *const task = slots[i].task.load(std::memory_order_acquire);
        if (task != nullptr) {
            destroyTaskEntry(*task);
            releaseTask(*task);
        }
    }
}

void SchedulerCore::notifyIdleWorker() noexcept {
    if (searching.load(std::memory_order_acquire) > 0) {
        return; // a searching worker will find the work without a futex
    }
    std::uint64_t mask = idleMask.load(std::memory_order_seq_cst);
    while (mask != 0) {
        auto const index = static_cast<std::size_t>(__builtin_ctzll(mask));
        std::uint64_t const bit = std::uint64_t{1} << index;
        if (idleMask.compare_exchange_weak(mask, mask & ~bit, std::memory_order_acq_rel)) {
            workers[index]->parker->unpark();
            return;
        }
    }
}

void SchedulerCore::notifyWorker(Worker &worker) noexcept {
    if (clearIdle(worker)) {
        worker.parker->unpark();
    }
    // Otherwise it is running and will drain its inbox at the next switch.
}

void SchedulerCore::markIdle(Worker &worker) noexcept {
    idleMask.fetch_or(std::uint64_t{1} << worker.index, std::memory_order_seq_cst);
}

bool SchedulerCore::clearIdle(Worker &worker) noexcept {
    std::uint64_t const bit = std::uint64_t{1} << worker.index;
    return (idleMask.fetch_and(~bit, std::memory_order_seq_cst) & bit) != 0;
}

Task *SchedulerCore::popInjection() noexcept {
    Task *task = nullptr;
    for (unsigned spins = 0; spins < 16; ++spins) {
        queue::PopStatus const status = injection.pop(task);
        if (status == queue::PopStatus::Ok) {
            return task;
        }
        if (status == queue::PopStatus::Empty) {
            return nullptr;
        }
        // Busy: a producer is between allocate and commit.
    }
    return nullptr;
}

void SchedulerCore::releaseTask(Task &task) noexcept {
    JoinState *const join = task.join;
#if STACKFULL_HAS_EXCEPTIONS
    std::exception_ptr escaped = std::move(task.exception);
    task.exception = nullptr;
    if (join != nullptr) {
        join->exception = std::move(escaped);
        finishJoin(*join);
    } else if (escaped) {
        exceptionSink->onUnhandledException(std::move(escaped));
    }
#else
    if (join != nullptr) {
        finishJoin(*join);
    }
#endif

    std::uint32_t const slot = task.slot;
    TaskSlot &entry = slots[slot];
    task.parkState.store(raw(TaskState::Done), std::memory_order_release);
    entry.task.store(nullptr, std::memory_order_release);
    entry.generation.fetch_add(1, std::memory_order_acq_rel);
    // Wakers that already hold the Task pointer finish within a few
    // instructions; wait them out before the memory goes away.
    while (entry.pins.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }

    stack::StackView const stack = task.stack;
    stack::StackAllocator *const stackAllocator = task.allocator;
    task.~Task();
    stackAllocator->deallocate(stack);
    releaseSlot(*this, slot);

    if (liveTasks.fetch_sub(1, std::memory_order_acq_rel) == 1 and stopping.load(std::memory_order_acquire)) {
        for (auto const &worker : workers) {
            worker->parker->unpark(); // let everyone observe "no tasks left" and exit
        }
    }
}

void SchedulerCore::wakeAllParked() noexcept {
    for (std::uint32_t i = 0; i < options.maxTasks; ++i) {
        TaskSlot &entry = slots[i];
        entry.pins.fetch_add(1, std::memory_order_acq_rel);
        Task *const task = entry.task.load(std::memory_order_acquire);
        if (task != nullptr) {
            wake(*task);
        }
        entry.pins.fetch_sub(1, std::memory_order_release);
    }
}

} // namespace detail
} // namespace sched
} // namespace stackfull
