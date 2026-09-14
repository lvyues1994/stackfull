#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/stack/StackAllocator.h>

#include <cstddef>
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

struct SchedulerOptions {
    // 0 selects std::thread::hardware_concurrency(). At most 64.
    std::size_t workers = 0;
    // Upper bound on simultaneously live tasks; sizes the injection queue and
    // the wake-token slab. spawn() fails with resource_unavailable_try_again
    // beyond it.
    std::size_t maxTasks = 65536;
    std::size_t taskStackSize = std::size_t{128} * 1024;
    // Borrowed; nullptr selects stack::defaultStackAllocator().
    stack::StackAllocator *allocator = nullptr;
    // Borrowed event source (the IO layer's Poller). nullptr: idle workers
    // sleep on futex/condvar only; timers still work.
    Driver *driver = nullptr;
#if STACKFULL_HAS_EXCEPTIONS
    // Borrowed; nullptr selects the aborting default.
    ExceptionSink *exceptionSink = nullptr;
#endif
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
