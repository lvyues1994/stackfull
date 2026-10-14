#pragma once

namespace stackfull {
namespace sched {

// A record that an integration (stackfull::exec) keeps for a task it runs,
// reachable from inside through this_task::context(). The integration owns
// it; the scheduler only calls onDropped. One integration per task.
struct TaskContext {
    // Without exceptions, Scheduler::stop() drops a task that is still parked
    // after its wake-up, without unwinding its stack; this runs first, on the
    // dropping worker. With exceptions the task unwinds through ForcedUnwind
    // instead and this is never called.
    void (*onDropped)(TaskContext &context) noexcept = nullptr;

protected:
    ~TaskContext() = default;
};

} // namespace sched
} // namespace stackfull
