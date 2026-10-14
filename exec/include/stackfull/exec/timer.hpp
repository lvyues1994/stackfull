#pragma once

#include <stackfull/exec/detail/waiting_operation.hpp>
#include <stackfull/exec/scheduler.hpp>

#include <lexec/core/completion_signatures.hpp>
#include <lexec/core/completion_tags.hpp>
#include <lexec/core/env.hpp>
#include <lexec/core/operation_state.hpp>
#include <lexec/core/receiver.hpp>
#include <lexec/core/sender.hpp>

#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sched/detail/TimerQueue.h>

#include <chrono>
#include <system_error>
#include <type_traits>

namespace stackfull::exec {

namespace detail {

// schedule() after a deadline: a timer in the scheduler's queues that, when
// it fires, spawns the task completing the receiver — so what follows runs
// inside a task, as after schedule(). Nothing is allocated; the timer entry
// lives in the operation. Stop requests take the timer back (set_stopped);
// Scheduler::stop() calls every such timer back at once, which completes
// with set_stopped on the thread calling stop().
template <class Rcvr>
struct timer_operation {
    using operation_state_concept = lexec::operation_state_t;
    using clock = std::chrono::steady_clock;

    timer_operation(sched::Scheduler *const target_, clock::time_point const at_, clock::duration const after_,
                    bool const relative_, Rcvr &&rcvr_) noexcept(std::is_nothrow_move_constructible_v<Rcvr>)
        : target(target_), at(at_), after(after_), relative(relative_), rcvr(static_cast<Rcvr &&>(rcvr_)) {}

    timer_operation(timer_operation &&) = delete;

    void start() & noexcept {
        if (lexec::get_stop_token(lexec::get_env(rcvr)).stop_requested() or target->stopRequested()) {
            lexec::set_stopped(static_cast<Rcvr &&>(rcvr));
            return;
        }
        entry.deadline = relative ? clock::now() + after : at;
        entry.callback = &fired;
        entry.argument = this;
        entry.queue = &target->callbackTimerQueue(); // before anything may cancel it
        listener.listen(rcvr, on_stop{this});
        if (not target->addCallbackTimer(entry)) {
            finish_stopped(); // cancelled while listening, or the scheduler is stopping
        }
    }

private:
    struct on_stop {
        void operator()() const noexcept {
            if (sched::detail::cancelTimer(op->entry)) {
                op->finish_stopped();
            }
        }

        timer_operation *op;
    };

    static void fired(void *const self) noexcept {
        auto &op = *static_cast<timer_operation *>(self);
        sched::SpawnResult const spawned = op.target->spawn([&op] { op.run(); });
        if (not spawned) {
            if (spawned.error == std::errc::operation_canceled) {
                op.finish_stopped();
            } else {
                op.listener.stop();
                lexec::set_error(static_cast<Rcvr &&>(op.rcvr), spawned.error);
            }
        }
    }

    void run() noexcept {
        // The receiver's code runs to its end, whatever stop() finds it doing.
        sched::this_task::NoForcedStop const shield;
        listener.stop();
        if (lexec::get_stop_token(lexec::get_env(rcvr)).stop_requested() or sched::this_task::stopRequested()) {
            lexec::set_stopped(static_cast<Rcvr &&>(rcvr));
        } else {
            lexec::set_value(static_cast<Rcvr &&>(rcvr));
        }
    }

    void finish_stopped() noexcept {
        listener.stop();
        lexec::set_stopped(static_cast<Rcvr &&>(rcvr));
    }

    sched::Scheduler *target;
    clock::time_point at;
    clock::duration after;
    bool relative;
    Rcvr rcvr;
    sched::detail::TimerEntry entry;
    stop_listener<Rcvr, on_stop> listener;
};

struct timer_sender {
    using sender_concept = lexec::sender_t;
    using completion_signatures = lexec::completion_signatures<lexec::set_value_t(), lexec::set_error_t(std::error_code),
                                                               lexec::set_stopped_t()>;

    template <class Rcvr>
    timer_operation<Rcvr> connect(Rcvr rcvr) const noexcept(std::is_nothrow_move_constructible_v<Rcvr>) {
        return {target, at, after, relative, static_cast<Rcvr &&>(rcvr)};
    }

    schedule_attrs get_env() const noexcept { return {target}; }

    sched::Scheduler *target;
    std::chrono::steady_clock::time_point at;
    std::chrono::steady_clock::duration after;
    bool relative;
};

} // namespace detail

// schedule(sch) once `delay` has passed since the operation started.
template <class Rep, class Period>
detail::timer_sender schedule_after(exec::scheduler const sch, std::chrono::duration<Rep, Period> const delay) noexcept {
    return {&sch.underlying(), {}, std::chrono::duration_cast<std::chrono::steady_clock::duration>(delay), true};
}

// schedule(sch) at `deadline`.
inline detail::timer_sender schedule_at(exec::scheduler const sch,
                                        std::chrono::steady_clock::time_point const deadline) noexcept {
    return {&sch.underlying(), deadline, {}, false};
}

} // namespace stackfull::exec
