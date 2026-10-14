#pragma once

#include <lexec/core/completion_signatures.hpp>
#include <lexec/core/completion_tags.hpp>
#include <lexec/core/env.hpp>
#include <lexec/core/operation_state.hpp>
#include <lexec/core/queries.hpp>
#include <lexec/core/receiver.hpp>
#include <lexec/core/scheduler.hpp>
#include <lexec/core/sender.hpp>

#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>

#include <system_error>
#include <type_traits>

namespace stackfull::exec {

struct scheduler;

namespace detail {

// Starting it spawns a task that completes the receiver: code after
// schedule() runs inside a task, so it may wait as tasks do.
template <class Rcvr>
struct schedule_operation {
    using operation_state_concept = lexec::operation_state_t;

    schedule_operation(sched::Scheduler *const target_, Rcvr &&rcvr_) noexcept(
        std::is_nothrow_move_constructible_v<Rcvr>)
        : target(target_), rcvr(static_cast<Rcvr &&>(rcvr_)) {}

    schedule_operation(schedule_operation &&) = delete;

    void start() & noexcept {
        sched::SpawnResult const spawned = target->spawn([this] { run(); });
        if (not spawned) {
            // Inline, on the starting thread: a stopping scheduler takes no
            // more tasks; anything else is a resource limit.
            if (spawned.error == std::errc::operation_canceled) {
                lexec::set_stopped(static_cast<Rcvr &&>(rcvr));
            } else {
                lexec::set_error(static_cast<Rcvr &&>(rcvr), spawned.error);
            }
        }
    }

private:
    void run() noexcept {
        // The receiver's code runs to its end, whatever stop() finds it doing.
        sched::this_task::NoForcedStop const shield;
        if (lexec::get_stop_token(lexec::get_env(rcvr)).stop_requested() or sched::this_task::stopRequested()) {
            lexec::set_stopped(static_cast<Rcvr &&>(rcvr));
        } else {
            lexec::set_value(static_cast<Rcvr &&>(rcvr));
        }
    }

    sched::Scheduler *target;
    Rcvr rcvr;
};

// Only values are sure to arrive on the scheduler: stopped and errors may
// come from start(), on the starting thread.
struct schedule_attrs {
    template <class Tag, class... Env, std::enable_if_t<std::is_same_v<Tag, lexec::set_value_t>, int> = 0>
    exec::scheduler query(lexec::get_completion_scheduler_t<Tag>, Env const &...) const noexcept;

    sched::Scheduler *target;
};

struct schedule_sender {
    using sender_concept = lexec::sender_t;
    using completion_signatures = lexec::completion_signatures<lexec::set_value_t(), lexec::set_error_t(std::error_code),
                                                               lexec::set_stopped_t()>;

    template <class Rcvr>
    schedule_operation<Rcvr> connect(Rcvr rcvr) const noexcept(std::is_nothrow_move_constructible_v<Rcvr>) {
        return {target, static_cast<Rcvr &&>(rcvr)};
    }

    schedule_attrs get_env() const noexcept { return {target}; }

    sched::Scheduler *target;
};

} // namespace detail

// A stackfull Scheduler as a lexec scheduler. schedule() spawns a task for
// each operation (a stack from the pool; nothing on the heap), and what
// follows runs inside that task.
struct scheduler {
    using scheduler_concept = lexec::scheduler_t;

    explicit scheduler(sched::Scheduler &target_) noexcept : target(&target_) {}

    detail::schedule_sender schedule() const noexcept { return {target}; }

    static constexpr lexec::forward_progress_guarantee query(lexec::get_forward_progress_guarantee_t) noexcept {
        return lexec::forward_progress_guarantee::parallel;
    }

    sched::Scheduler &underlying() const noexcept { return *target; }

    friend bool operator==(scheduler const lhs, scheduler const rhs) noexcept { return lhs.target == rhs.target; }
    friend bool operator!=(scheduler const lhs, scheduler const rhs) noexcept { return lhs.target != rhs.target; }

private:
    sched::Scheduler *target;
};

template <class Tag, class... Env, std::enable_if_t<std::is_same_v<Tag, lexec::set_value_t>, int>>
exec::scheduler detail::schedule_attrs::query(lexec::get_completion_scheduler_t<Tag>, Env const &...) const noexcept {
    return exec::scheduler{*target};
}

} // namespace stackfull::exec
