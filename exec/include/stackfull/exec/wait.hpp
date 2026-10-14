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
#include <lexec/core/sender_traits.hpp>
#include <lexec/detail/config.hpp>
#include <lexec/stop_token.hpp>

#include <stackfull/coro/Fatal.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sched/WakeToken.h>

#include <atomic>
#include <exception>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace stackfull::exec {

namespace detail {

// What a sender waited for in a task sees: the task's stop token, and the
// task's scheduler as the current, starting and delegation scheduler.
struct task_env {
    lexec::inplace_stop_token query(lexec::get_stop_token_t) const noexcept { return token; }
    exec::scheduler query(lexec::get_scheduler_t) const noexcept { return exec::scheduler{*target}; }
    exec::scheduler query(lexec::get_start_scheduler_t) const noexcept { return exec::scheduler{*target}; }
    exec::scheduler query(lexec::get_delegation_scheduler_t) const noexcept { return exec::scheduler{*target}; }

    lexec::inplace_stop_token token;
    sched::Scheduler *target;
};

template <class... ValueTuples>
struct single_value_tuple {
    static_assert(sizeof...(ValueTuples) == 1,
                  "stackfull::exec::wait requires a sender with exactly one value completion signature");
};

template <class ValueTuple>
struct single_value_tuple<ValueTuple> {
    using type = ValueTuple;
};

template <class... ValueTuples>
using single_value_tuple_t = typename single_value_tuple<ValueTuples...>::type;

template <class Sndr>
using wait_result_t = std::optional<lexec::value_types_of_t<Sndr, task_env, lexec::decayed_tuple, single_value_tuple_t>>;

template <class Sndr>
using wait_with_variant_result_t = std::optional<lexec::value_types_of_t<Sndr, task_env>>;

struct wait_state {
    std::atomic<bool> done{false};
    sched::WakeToken waiter;
    lexec::inplace_stop_token token;
    sched::Scheduler *target = nullptr;
#if LEXEC_HAS_EXCEPTIONS
    std::exception_ptr error;
#endif
};

// The wait whose operation this thread is starting: a completion inside
// start() needs no wake-up (start() runs to its end without parking).
inline thread_local wait_state *starting_wait = nullptr;

template <class Result, bool InVariant>
struct wait_receiver {
    using receiver_concept = lexec::receiver_t;

    template <class... Vs>
    void set_value(Vs &&...vs) && noexcept {
#if LEXEC_HAS_EXCEPTIONS
        try {
            store(static_cast<Vs &&>(vs)...);
        } catch (...) {
            state->error = std::current_exception();
        }
#else
        store(static_cast<Vs &&>(vs)...);
#endif
        finish();
    }

    template <class E>
    void set_error(E &&error) && noexcept {
#if LEXEC_HAS_EXCEPTIONS
        state->error = as_exception_ptr(static_cast<E &&>(error));
        finish();
#else
        static_cast<void>(error);
        std::terminate(); // as lexec::sync_wait without exceptions
#endif
    }

    void set_stopped() && noexcept { finish(); }

    task_env get_env() const noexcept { return task_env{state->token, state->target}; }

    template <class... Vs>
    void store(Vs &&...vs) {
        if constexpr (InVariant) {
            result->emplace(std::in_place_type<lexec::decayed_tuple<Vs...>>, static_cast<Vs &&>(vs)...);
        } else {
            result->emplace(static_cast<Vs &&>(vs)...);
        }
    }

    // Once `done` is set the task may return and take the state with it.
    void finish() noexcept {
        if (starting_wait == state) {
            state->done.store(true, std::memory_order_relaxed);
            return;
        }
        sched::WakeToken const waiter = state->waiter;
        state->done.store(true, std::memory_order_release);
        waiter.wake();
    }

    wait_state *state;
    Result *result;
};

template <class Result, bool InVariant, class Sndr>
Result run_wait(Sndr &&sndr) {
    STACKFULL_CHECK(sched::this_task::isTask(),
                    "stackfull::exec::wait: call it from a task; plain threads use lexec::sync_wait");
    // The operation lives in this frame until it completes.
    sched::this_task::NoForcedStop const shield;
    lexec::inplace_stop_source own;
    lexec::inplace_stop_source *const task_source = task_stop_source();
    lexec::inplace_stop_source &source = task_source != nullptr ? *task_source : own;
    wait_state state;
    state.waiter = sched::this_task::token();
    state.token = source.get_token();
    state.target = &sched::this_task::scheduler();
    Result result{};
    {
        auto op = lexec::connect(static_cast<Sndr &&>(sndr), wait_receiver<Result, InVariant>{&state, &result});
        wait_state *const outer = starting_wait;
        starting_wait = &state;
        lexec::start(op);
        starting_wait = outer;
        // Scheduler::stop() wakes every task once: pass it on to the sender.
        bool forwarded = false;
        while (not state.done.load(std::memory_order_acquire)) {
            if (not forwarded and sched::this_task::stopRequested()) {
                forwarded = true;
                source.request_stop();
                continue;
            }
            sched::this_task::park();
        }
    }
#if LEXEC_HAS_EXCEPTIONS
    if (state.error) {
        std::rethrow_exception(state.error);
    }
#endif
    return result;
}

#if LEXEC_HAS_EXCEPTIONS
template <class Tuple>
struct awaited;

template <>
struct awaited<std::tuple<>> {
    using type = void;
    static void unpack(std::tuple<> &&) noexcept {}
};

template <class V>
struct awaited<std::tuple<V>> {
    using type = V;
    static V unpack(std::tuple<V> &&values) { return std::get<0>(static_cast<std::tuple<V> &&>(values)); }
};

template <class V, class W, class... Vs>
struct awaited<std::tuple<V, W, Vs...>> {
    using type = std::tuple<V, W, Vs...>;
    static type unpack(type &&values) { return static_cast<type &&>(values); }
};

template <class Sndr>
using awaited_t = awaited<typename wait_result_t<Sndr>::value_type>;
#endif

} // namespace detail

// Waits in the calling task for `sndr`, which may complete on any thread;
// the task continues on its own scheduler. The operation lives in this frame
// (nothing is allocated) and the sender sees the task's stop token and
// scheduler. Like lexec::sync_wait: the values, an empty optional when
// stopped, the error rethrown (std::terminate without exceptions).
// Scheduler::stop() does not unwind a task here; it asks the sender to stop
// and waits for it to complete. Tasks only — plain threads use
// lexec::sync_wait.
template <class Sndr>
auto wait(Sndr &&sndr) -> detail::wait_result_t<Sndr> {
    static_assert(lexec::is_sender_in_v<Sndr, detail::task_env>,
                  "stackfull::exec::wait: the sender's completion signatures cannot be computed");
    return detail::run_wait<detail::wait_result_t<Sndr>, false>(static_cast<Sndr &&>(sndr));
}

// wait() for a sender with any number of value completions: a std::variant
// of std::tuple, one alternative for each.
template <class Sndr>
auto wait_with_variant(Sndr &&sndr) -> detail::wait_with_variant_result_t<Sndr> {
    static_assert(lexec::is_sender_in_v<Sndr, detail::task_env>,
                  "stackfull::exec::wait_with_variant: the sender's completion signatures cannot be computed");
    return detail::run_wait<detail::wait_with_variant_result_t<Sndr>, true>(static_cast<Sndr &&>(sndr));
}

#if LEXEC_HAS_EXCEPTIONS
// wait() shaped like co_await: the one value, void, or a tuple of the
// values; stopped throws stopped_error.
template <class Sndr>
auto await(Sndr &&sndr) -> typename detail::awaited_t<Sndr>::type {
    auto result = exec::wait(static_cast<Sndr &&>(sndr));
    if (not result) {
        throw stopped_error{};
    }
    return detail::awaited_t<Sndr>::unpack(static_cast<typename decltype(result)::value_type &&>(*result));
}
#endif

} // namespace stackfull::exec
