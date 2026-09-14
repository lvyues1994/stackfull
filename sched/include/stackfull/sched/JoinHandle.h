#pragma once

namespace stackfull {
namespace sched {

namespace detail {
struct JoinState;
}

struct Scheduler;

// Handle returned by Scheduler::spawnJoinable(). Move-only. Dropping it
// detaches the task (it keeps running); it never blocks in the destructor.
struct JoinHandle {
    JoinHandle() noexcept = default;
    JoinHandle(JoinHandle const &) = delete;
    JoinHandle &operator=(JoinHandle const &) = delete;
    JoinHandle(JoinHandle &&other) noexcept : state(other.state) { other.state = nullptr; }
    JoinHandle &operator=(JoinHandle &&other) noexcept;
    ~JoinHandle() { detach(); }

    explicit operator bool() const noexcept { return state != nullptr; }

    // Wait for the task to finish. Parks when called from a task, blocks the
    // OS thread otherwise. With exceptions enabled, rethrows whatever
    // escaped the task body. Only one joiner per task.
    void join();

    bool isDone() const noexcept;

    // Give up the handle without waiting.
    void detach() noexcept;

private:
    friend struct Scheduler;
    explicit JoinHandle(detail::JoinState *const state_) noexcept : state(state_) {}

    detail::JoinState *state = nullptr;
};

} // namespace sched
} // namespace stackfull
