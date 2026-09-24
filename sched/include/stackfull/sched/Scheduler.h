#pragma once

#include <stackfull/sched/JoinHandle.h>
#include <stackfull/sched/SchedulerOptions.h>
#include <stackfull/sched/Stats.h>
#include <stackfull/sched/detail/JoinState.h>
#include <stackfull/sched/detail/Runtime.h>
#include <stackfull/sched/detail/TaskFactory.h>

#include <cstddef>
#include <functional>
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
// makeScheduler() returns a *running* scheduler: spawn() right away. With
// SchedulerOptions::callerIsWorker one worker is held back for the thread
// that calls run().
//
//   run()    (callerIsWorker only) turns the calling thread into the
//            reserved worker until the scheduler has stopped.
//   start()  starts any worker not yet running; a no-op in the default
//            configuration, kept so code written for explicit start works.
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

    // Counters (and, if enabled, the wake-latency histogram), summed over
    // workers. Thread-safe; a snapshot taken while the scheduler runs.
    virtual SchedulerStats stats() const = 0;
    // Calls `visit` once for every live task, e.g. to dump what is parked.
    // Each TaskInfo is a copy taken at that moment; the task may have moved
    // on by the time `visit` sees it. Thread-safe.
    virtual void forEachTask(std::function<void(TaskInfo const &)> const &visit) const = 0;

protected:
    virtual detail::SchedulerCore &core() noexcept = 0;
};

std::unique_ptr<Scheduler> makeScheduler(SchedulerOptions const &options = SchedulerOptions{});

} // namespace sched
} // namespace stackfull
