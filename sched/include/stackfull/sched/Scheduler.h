#pragma once

#include <stackfull/sched/JoinHandle.h>
#include <stackfull/sched/SchedulerOptions.h>
#include <stackfull/sched/detail/JoinState.h>
#include <stackfull/sched/detail/Runtime.h>
#include <stackfull/sched/detail/TaskFactory.h>

#include <cstddef>
#include <memory>
#include <system_error>
#include <utility>

namespace stackfull {
namespace sched {

struct JoinableSpawnResult {
    JoinHandle handle;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// M:N scheduler: N worker threads run M tasks, each a stackful coroutine
// that may suspend on any worker and continue on another.
//
//   start()  launches all workers in the background.
//   run()    launches all but one worker and runs the last on the calling
//            thread until the scheduler has stopped.
//   stop()   requests shutdown from any thread and returns at once: spawn()
//            begins to fail, every parked task is woken once so cooperative
//            code sees this_task::stopRequested(), and tasks still parked
//            afterwards are unwound (ForcedUnwind) — or, without exceptions,
//            released without running their destructors. Workers exit when
//            no task is left. A task that never parks cannot be stopped.
//
// The destructor requests stop() and joins the background threads.
struct Scheduler {
    virtual ~Scheduler() = default;

    // Fire-and-forget task. Thread-safe.
    template <class F>
    SpawnResult spawn(F &&body, TaskOptions const &options = TaskOptions{}) {
        detail::TaskCreation const created = detail::createTask(core(), std::forward<F>(body), options, nullptr);
        if (created.error) {
            return SpawnResult{created.error};
        }
        core().schedule(*created.task);
        return SpawnResult{};
    }

    // Task with a handle to wait for (and, with exceptions, observe failures of).
    template <class F>
    JoinableSpawnResult spawnJoinable(F &&body, TaskOptions const &options = TaskOptions{}) {
        auto state = std::make_unique<detail::JoinState>();
        detail::TaskCreation const created =
            detail::createTask(core(), std::forward<F>(body), options, state.get());
        if (created.error) {
            return JoinableSpawnResult{JoinHandle{}, created.error};
        }
        JoinableSpawnResult result{JoinHandle{state.release()}, std::error_code{}};
        core().schedule(*created.task);
        return result;
    }

    virtual void start() = 0;
    virtual void run() = 0;
    virtual void stop() noexcept = 0;

    virtual std::size_t workerCount() const noexcept = 0;
    // Tasks created and not yet finished.
    virtual std::size_t liveTasks() const noexcept = 0;

protected:
    virtual detail::SchedulerCore &core() noexcept = 0;
};

std::unique_ptr<Scheduler> makeScheduler(SchedulerOptions const &options = SchedulerOptions{});

} // namespace sched
} // namespace stackfull
