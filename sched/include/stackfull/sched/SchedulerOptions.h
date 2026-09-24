#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/stack/StackAllocator.h>

#include <chrono>
#include <cstddef>
#include <functional>
#include <system_error>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

namespace stackfull {
namespace sched {

struct Driver;

#if STACKFULL_HAS_EXCEPTIONS
// Receives exceptions that escape a *detached* task (a joinable task's
// exception is rethrown by join() instead). The default aborts the process,
// mirroring std::thread.
struct ExceptionSink {
    virtual ~ExceptionSink() = default;
    virtual void onUnhandledException(std::exception_ptr exception) noexcept = 0;
};
#endif

// Told about a worker that has been running one task for longer than
// SchedulerOptions::stallThreshold without it yielding, parking or finishing.
// Called on the scheduler's monitor thread.
struct StallSink {
    virtual ~StallSink() = default;
    virtual void onStall(std::size_t workerIndex, std::chrono::milliseconds stalledFor) noexcept = 0;
};

struct SchedulerOptions {
    // 0 selects std::thread::hardware_concurrency(). At most 64.
    std::size_t workers = 0;
    // Upper bound on simultaneously live tasks; sizes the wake-token slab.
    // spawn() fails with resource_unavailable_try_again beyond it. At most
    // the build's STACKFULL_SCHED_TASK_CAPACITY (126976 by default).
    std::size_t maxTasks = 65536;
    std::size_t taskStackSize = std::size_t{128} * 1024;
    // Stacks of taskStackSize the allocator sets aside at construction
    // (StackAllocator::reserve): a burst of spawns then maps no memory.
    std::size_t reserveStacks = 0;
    // Write a canary at the bottom of every task stack and check it each
    // time a task switches out; a clobbered canary aborts with "stack
    // overflow". Meant for allocators without guard pages
    // (MmapStackOptions::guardPages = 0), where an overflow is otherwise silent.
    bool checkStackCanary = false;
    // Borrowed; nullptr selects stack::defaultStackAllocator().
    stack::StackAllocator *allocator = nullptr;
    // Borrowed event source (the IO layer's Poller). nullptr: idle workers
    // sleep on futex/condvar only; timers still work.
    Driver *driver = nullptr;
    // false: makeScheduler() starts every worker thread at once.
    // true:  one worker is reserved for the thread that calls run(); the
    //        others start immediately.
    bool callerIsWorker = false;
#if STACKFULL_HAS_EXCEPTIONS
    // Borrowed; nullptr selects the aborting default.
    ExceptionSink *exceptionSink = nullptr;
#endif
    // Runs on each worker thread, with its index, before the worker takes
    // any task: set CPU affinity (pinCurrentThreadToCpus), priority or
    // scheduling policy here. Must not throw.
    std::function<void(std::size_t workerIndex)> onWorkerStart;
    // A worker that keeps running one task this long without a switch is
    // reported to stallSink, once per episode. 0 disables the monitor thread
    // (which otherwise wakes every stallThreshold / 2).
    std::chrono::milliseconds stallThreshold{0};
    // Borrowed; nullptr prints to stderr.
    StallSink *stallSink = nullptr;
    // When a worker finds work and more is left over, the next idle worker
    // joins only after this long, and only if the backlog is still there:
    // a burst of short tasks is then drained by the workers already awake
    // instead of waking one after another. The first helper for a batch is
    // always woken at once. 0 restores immediate ramp-up.
    std::chrono::microseconds rampUpDelay{50};
};

struct TaskOptions {
    // 0 selects SchedulerOptions::taskStackSize.
    std::size_t stackSize = 0;
};

struct SpawnResult {
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

} // namespace sched
} // namespace stackfull
