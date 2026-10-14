#pragma once

#include <lexec/core/env.hpp>
#include <lexec/core/operation_state.hpp>
#include <lexec/core/queries.hpp>
#include <lexec/core/receiver.hpp>
#include <lexec/stop_token.hpp>

#include <stackfull/sync/detail/Waiter.h>

#include <new>
#include <type_traits>

namespace stackfull::exec::detail {

// A stop callback on the receiver's token for part of an operation's life.
// stop() may run on any thread, also inside the callback itself; once it
// returns the callback is not running and will not run.
template <class Rcvr, class OnStop>
struct stop_listener {
    using token_type = lexec::stop_token_of_t<lexec::env_of_t<Rcvr>>;
    static constexpr bool stoppable = not lexec::is_unstoppable_token_v<token_type>;
    using callback_type = lexec::stop_callback_for_t<token_type, OnStop>;

    stop_listener() noexcept = default;
    stop_listener(stop_listener &&) = delete;
    ~stop_listener() { stop(); }

    // May run `on_stop` at once, inside, if stop was requested already.
    void listen(Rcvr const &rcvr, OnStop const on_stop) noexcept {
        if constexpr (stoppable) {
            ::new (static_cast<void *>(storage)) callback_type(lexec::get_stop_token(lexec::get_env(rcvr)), on_stop);
            active = true;
        }
    }

    void stop() noexcept {
        if constexpr (stoppable) {
            if (active) {
                active = false;
                std::launder(reinterpret_cast<callback_type *>(storage))->~callback_type();
            }
        }
    }

private:
    alignas(callback_type) unsigned char storage[stoppable ? sizeof(callback_type) : 1];
    bool active = false;
};

// Base of the operations that wait with a callback waiter on a sync
// primitive or a descriptor. Derived provides, publicly:
//   bool try_now() noexcept      complete at once if it can (true), queueing
//                                nothing; runs before the stop callback exists
//   void drive() noexcept        try, else queue the waiter (again); complete
//                                when done or when the queueing is Cancelled
//   bool cancel_wait() noexcept  take the waiter back: true if it was queued
//   void on_notified() noexcept  the waiter's callback (default: drive())
//
// Completion is inline, on whichever thread made it possible: the starting
// thread, a notifier (a task, a plain thread, the IO polling thread) or the
// one requesting stop. Once the waiter is queued, the code that queued it
// does not touch the operation again: the callback may already have run.
template <class Derived, class Rcvr>
struct waiting_operation {
    using operation_state_concept = lexec::operation_state_t;

    explicit waiting_operation(Rcvr &&rcvr_) noexcept(std::is_nothrow_move_constructible_v<Rcvr>)
        : rcvr(static_cast<Rcvr &&>(rcvr_)) {}
    waiting_operation(waiting_operation &&) = delete;

    void start() & noexcept {
        if (lexec::get_stop_token(lexec::get_env(rcvr)).stop_requested()) {
            lexec::set_stopped(static_cast<Rcvr &&>(rcvr));
            return;
        }
        if (self().try_now()) {
            return;
        }
        // Before the first queueing: a stop already requested then only
        // marks the waiter, and the queueing reports Cancelled.
        listener.listen(rcvr, on_stop{this});
        self().drive();
    }

    bool try_now() noexcept { return false; }
    void on_notified() noexcept { self().drive(); }

    template <class... Vs>
    void finish_value(Vs &&...values) noexcept {
        listener.stop();
        lexec::set_value(static_cast<Rcvr &&>(rcvr), static_cast<Vs &&>(values)...);
    }
    template <class E>
    void finish_error(E &&error) noexcept {
        listener.stop();
        lexec::set_error(static_cast<Rcvr &&>(rcvr), static_cast<E &&>(error));
    }
    void finish_stopped() noexcept {
        listener.stop();
        lexec::set_stopped(static_cast<Rcvr &&>(rcvr));
    }

    // Ends a drive() step on a queueing that moves nothing (Ready means: do
    // it now).
    template <class Done>
    void settle(sync::detail::EnqueueResult const result, Done const &done) noexcept {
        switch (result) {
        case sync::detail::EnqueueResult::Ready:
            done();
            return;
        case sync::detail::EnqueueResult::Enqueued:
            return;
        case sync::detail::EnqueueResult::Cancelled:
            finish_stopped();
            return;
        }
    }

    Rcvr rcvr;
    sync::detail::Waiter waiter{&notified, this};

private:
    struct on_stop {
        void operator()() const noexcept {
            if (op->self().cancel_wait()) {
                op->finish_stopped();
            }
        }

        waiting_operation *op;
    };

    static void notified(void *const op) noexcept { static_cast<waiting_operation *>(op)->self().on_notified(); }
    Derived &self() noexcept { return static_cast<Derived &>(*this); }

    stop_listener<Rcvr, on_stop> listener;
};

} // namespace stackfull::exec::detail
