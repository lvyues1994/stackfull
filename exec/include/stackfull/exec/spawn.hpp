#pragma once

#include <stackfull/exec/detail/task_context.hpp>
#include <stackfull/exec/scheduler.hpp>

#include <lexec/core/completion_signatures.hpp>
#include <lexec/core/completion_tags.hpp>
#include <lexec/core/env.hpp>
#include <lexec/core/operation_state.hpp>
#include <lexec/core/queries.hpp>
#include <lexec/core/receiver.hpp>
#include <lexec/core/sender.hpp>
#include <lexec/detail/config.hpp>
#include <lexec/stop_token.hpp>

#include <stackfull/coro/ForcedUnwind.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/SchedulerOptions.h>
#include <stackfull/sched/TaskContext.h>
#include <stackfull/sched/ThisTask.h>

#include <exception>
#include <new>
#include <system_error>
#include <type_traits>
#include <utility>

namespace stackfull::exec {

namespace detail {

// Runs `fn` in a task of its own and completes the receiver at the end of it:
// with fn's value; its exception, or stopped for stopped_error; stopped when
// Scheduler::stop() ends the task early; an error_code when no task could be
// spawned. The receiver's stop requests reach the task's stop source, which
// the senders the task waits for (exec::wait) see.
template <class Fn, class Rcvr>
struct spawn_operation : task_context {
    using operation_state_concept = lexec::operation_state_t;
    using result_type = std::decay_t<std::invoke_result_t<Fn &>>;
    using receiver_token = lexec::stop_token_of_t<lexec::env_of_t<Rcvr>>;
    static constexpr bool forwards_stop = not lexec::is_unstoppable_token_v<receiver_token>;

    struct forward_stop {
        void operator()() noexcept { source->request_stop(); }

        lexec::inplace_stop_source *source;
    };

    using forward_callback = lexec::stop_callback_for_t<receiver_token, forward_stop>;

    spawn_operation(sched::Scheduler *const target_, sched::TaskOptions const &options_, Fn &&fn_, Rcvr &&rcvr_) noexcept(
        std::is_nothrow_move_constructible_v<Fn> and std::is_nothrow_move_constructible_v<Rcvr>)
        : target(target_), options(options_), fn(static_cast<Fn &&>(fn_)), rcvr(static_cast<Rcvr &&>(rcvr_)) {
        stop_source = &source;
        onDropped = &dropped;
    }

    spawn_operation(spawn_operation &&) = delete;

    ~spawn_operation() { stop_forwarding(); }

    void start() & noexcept {
        if constexpr (forwards_stop) {
            ::new (static_cast<void *>(callback_storage))
                forward_callback(lexec::get_stop_token(lexec::get_env(rcvr)), forward_stop{&source});
            forwarding = true;
        }
        sched::SpawnResult const spawned = target->spawn([this] { run(); }, options);
        if (not spawned) {
            stop_forwarding();
            if (spawned.error == std::errc::operation_canceled) {
                lexec::set_stopped(static_cast<Rcvr &&>(rcvr));
            } else {
                lexec::set_error(static_cast<Rcvr &&>(rcvr), spawned.error);
            }
        }
    }

private:
    void run() {
        sched::this_task::setContext(this);
#if LEXEC_HAS_EXCEPTIONS
        try {
            run_fn();
        } catch (coro::ForcedUnwind const &) {
            // Scheduler::stop() unwinds the task: the receiver hears first,
            // then the unwinding goes on.
            complete([this] { lexec::set_stopped(static_cast<Rcvr &&>(rcvr)); });
            throw;
        } catch (stopped_error const &) {
            complete([this] { lexec::set_stopped(static_cast<Rcvr &&>(rcvr)); });
        } catch (...) {
            complete([this, error = std::current_exception()]() mutable {
                lexec::set_error(static_cast<Rcvr &&>(rcvr), static_cast<std::exception_ptr &&>(error));
            });
        }
#else
        run_fn();
#endif
    }

    void run_fn() {
        if constexpr (std::is_void_v<result_type>) {
            fn();
            complete([this] { lexec::set_value(static_cast<Rcvr &&>(rcvr)); });
        } else {
            result_type value = fn();
            complete([this, &value] { lexec::set_value(static_cast<Rcvr &&>(rcvr), static_cast<result_type &&>(value)); });
        }
    }

    // In the task: nothing here is touched after the receiver is called,
    // which may destroy this operation.
    template <class Completion>
    void complete(Completion &&completion) noexcept {
        stop_forwarding();
        sched::this_task::setContext(nullptr);
        // The receiver's code runs to its end, whatever stop() finds it doing.
        sched::this_task::NoForcedStop const shield;
        completion();
    }

    // Without exceptions: Scheduler::stop() dropped the parked task.
    static void dropped(sched::TaskContext &context) noexcept {
        auto &self = static_cast<spawn_operation &>(static_cast<task_context &>(context));
        self.stop_forwarding();
        lexec::set_stopped(static_cast<Rcvr &&>(self.rcvr));
    }

    void stop_forwarding() noexcept {
        if constexpr (forwards_stop) {
            if (forwarding) {
                forwarding = false;
                std::launder(reinterpret_cast<forward_callback *>(callback_storage))->~forward_callback();
            }
        }
    }

    sched::Scheduler *target;
    sched::TaskOptions options;
    Fn fn;
    Rcvr rcvr;
    lexec::inplace_stop_source source;
    alignas(forward_callback) unsigned char callback_storage[forwards_stop ? sizeof(forward_callback) : 1];
    bool forwarding = false;
};

template <class R>
struct spawn_value_signature {
    using type = lexec::set_value_t(R);
};

template <>
struct spawn_value_signature<void> {
    using type = lexec::set_value_t();
};

template <class Fn>
struct spawn_sender {
    using sender_concept = lexec::sender_t;
    using result_type = std::decay_t<std::invoke_result_t<Fn &>>;
#if LEXEC_HAS_EXCEPTIONS
    using completion_signatures =
        lexec::completion_signatures<typename spawn_value_signature<result_type>::type,
                                     lexec::set_error_t(std::exception_ptr), lexec::set_error_t(std::error_code),
                                     lexec::set_stopped_t()>;
#else
    using completion_signatures =
        lexec::completion_signatures<typename spawn_value_signature<result_type>::type,
                                     lexec::set_error_t(std::error_code), lexec::set_stopped_t()>;
#endif

    template <class Rcvr>
    spawn_operation<Fn, Rcvr> connect(Rcvr rcvr) && noexcept(std::is_nothrow_move_constructible_v<Fn> and
                                                             std::is_nothrow_move_constructible_v<Rcvr>) {
        return {target, options, static_cast<Fn &&>(fn), static_cast<Rcvr &&>(rcvr)};
    }

    template <class Rcvr, class F = Fn, std::enable_if_t<std::is_copy_constructible_v<F>, int> = 0>
    spawn_operation<Fn, Rcvr> connect(Rcvr rcvr) const & {
        return {target, options, Fn(fn), static_cast<Rcvr &&>(rcvr)};
    }

    sched::Scheduler *target;
    sched::TaskOptions options;
    Fn fn;
};

} // namespace detail

// A sender that runs `fn` in a task of the scheduler when started and
// completes with its result at the end of that task (see spawn_operation).
// Each start spawns a task; nothing is allocated besides its pooled stack.
template <class Fn>
detail::spawn_sender<std::decay_t<Fn>> spawn(exec::scheduler const sch, Fn &&fn,
                                             sched::TaskOptions const &options = sched::TaskOptions{}) {
    return detail::spawn_sender<std::decay_t<Fn>>{&sch.underlying(), options, static_cast<Fn &&>(fn)};
}

} // namespace stackfull::exec
