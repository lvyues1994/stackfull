#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

namespace stackfull {
namespace sync {

// What a Completion delivers: a value, or an error, or (with exceptions
// enabled) an exception captured on the producing side.
template <class T>
struct Outcome {
    T value{};
    std::error_code error;
#if STACKFULL_HAS_EXCEPTIONS
    std::exception_ptr exception;
    explicit operator bool() const noexcept { return not error and not exception; }
#else
    explicit operator bool() const noexcept { return not error; }
#endif
};

template <>
struct Outcome<void> {
    std::error_code error;
#if STACKFULL_HAS_EXCEPTIONS
    std::exception_ptr exception;
    explicit operator bool() const noexcept { return not error and not exception; }
#else
    explicit operator bool() const noexcept { return not error; }
#endif
};

namespace detail {

// Shared state of a Completion. The awaiter is recorded as a Waker *by
// value* and woken under the lock; readiness lives here, not in the
// awaiter's stack frame, so the producer never dereferences memory owned
// by a task that may already be gone. One awaiter may register with
// several sources at once (select).
template <class T>
struct CompletionState {
    SpinLock lock;
    Waker consumer;
    std::atomic<bool> claimed{false}; // first set()/fail() wins
    std::atomic<bool> ready{false};
    Outcome<T> outcome;

    bool claim() noexcept { return not claimed.exchange(true, std::memory_order_acq_rel); }

    void publish() noexcept {
        Waker toWake;
        {
            SpinLockGuard const guard(lock);
            ready.store(true, std::memory_order_release);
            toWake = consumer;
        }
        toWake.wake();
    }

    // False if already ready (nothing to wait for).
    bool attach(Waiter const &w) noexcept {
        SpinLockGuard const guard(lock);
        if (ready.load(std::memory_order_acquire)) {
            return false;
        }
        consumer = w.waker();
        return true;
    }

    void detach(Waiter const &w) noexcept {
        SpinLockGuard const guard(lock);
        if (consumer == w.waker()) {
            consumer.clear();
        }
    }
};

template <class T>
struct DetachOnExit {
    CompletionState<T> &state;
    Waiter &waiter;
    ~DetachOnExit() { state.detach(waiter); }
};

} // namespace detail

// One-shot result shared between whoever produces it (an SDK callback on
// any thread) and whoever waits for it (a task, or a plain thread).
//
// A Completion is a cheap, copyable handle; every copy refers to the same
// state, so the callback side may outlive the awaiting task — the task may
// be unwound by Scheduler::stop() or give up with getFor(), and a late
// set() just lands in state nobody reads.
//
// It is itself callable: pass it where the SDK wants a callback. Whatever
// arguments the SDK passes construct T by brace-initialisation, so T is the
// only thing that has to match the SDK's signature (a value, a std::pair,
// a std::tuple, your own struct). completionFor<Signature>() deduces T.
//
// Single consumer: get() moves the outcome out. Safe in no-exceptions
// builds too: the state never points into the awaiting task's stack.
template <class T>
struct Completion {
    Completion() : state(std::make_shared<detail::CompletionState<T>>()) {}

    // --- producer side (any thread; const because the handle is shared) ---
    template <class... Args>
    void set(Args &&...args) const {
        if (not state->claim()) {
            return;
        }
        state->outcome.value = T{std::forward<Args>(args)...};
        state->publish();
    }

    void fail(std::error_code const error) const {
        if (not state->claim()) {
            return;
        }
        state->outcome.error = error;
        state->publish();
    }

#if STACKFULL_HAS_EXCEPTIONS
    void setException(std::exception_ptr exception) const {
        if (not state->claim()) {
            return;
        }
        state->outcome.exception = std::move(exception);
        state->publish();
    }
#endif

    template <class... Args>
    void operator()(Args &&...args) const {
        set(std::forward<Args>(args)...);
    }

    // --- consumer side ----------------------------------------------------
    bool isReady() const noexcept { return state->ready.load(std::memory_order_acquire); }

    Outcome<T> get() const {
        if (not isReady()) {
            detail::Waiter waiter;
            detail::DetachOnExit<T> const detach{*state, waiter};
            while (state->attach(waiter)) {
                waiter.block(); // spurious wakeups just re-attach
            }
        }
        return std::move(state->outcome);
    }

    // Times out with std::errc::timed_out; the producer may still set later.
    Outcome<T> getUntil(std::chrono::steady_clock::time_point const deadline) const {
        if (not isReady()) {
            detail::Waiter waiter;
            detail::DetachOnExit<T> const detach{*state, waiter};
            while (state->attach(waiter)) {
                if (not waiter.blockUntil(deadline)) {
                    break;
                }
            }
            if (not isReady()) {
                Outcome<T> timedOut;
                timedOut.error = std::make_error_code(std::errc::timed_out);
                return timedOut;
            }
        }
        return std::move(state->outcome);
    }

    template <class Rep, class Period>
    Outcome<T> getFor(std::chrono::duration<Rep, Period> const timeout) const {
        return getUntil(std::chrono::steady_clock::now() +
                        std::chrono::duration_cast<std::chrono::nanoseconds>(timeout));
    }

    // --- C callbacks: carry the handle through a void *user pointer --------
    void *toRaw() const { return new Completion(*this); }
    static Completion fromRaw(void *const raw) {
        std::unique_ptr<Completion> const owned(static_cast<Completion *>(raw));
        return *owned;
    }

    // --- select() support ----------------------------------------------------
    bool attach(detail::Waiter &waiter) const noexcept { return state->attach(waiter); }
    void detach(detail::Waiter &waiter) const noexcept { state->detach(waiter); }

private:
    std::shared_ptr<detail::CompletionState<T>> state;
};

// Completion<void>: a signal. Any callback arguments are accepted and ignored.
template <>
struct Completion<void> {
    Completion() : state(std::make_shared<detail::CompletionState<void>>()) {}

    template <class... Args>
    void set(Args &&...) const {
        if (not state->claim()) {
            return;
        }
        state->publish();
    }

    void fail(std::error_code const error) const {
        if (not state->claim()) {
            return;
        }
        state->outcome.error = error;
        state->publish();
    }

#if STACKFULL_HAS_EXCEPTIONS
    void setException(std::exception_ptr exception) const {
        if (not state->claim()) {
            return;
        }
        state->outcome.exception = std::move(exception);
        state->publish();
    }
#endif

    template <class... Args>
    void operator()(Args &&...args) const {
        set(std::forward<Args>(args)...);
    }

    bool isReady() const noexcept { return state->ready.load(std::memory_order_acquire); }

    Outcome<void> get() const {
        if (not isReady()) {
            detail::Waiter waiter;
            detail::DetachOnExit<void> const detach{*state, waiter};
            while (state->attach(waiter)) {
                waiter.block();
            }
        }
        return state->outcome;
    }

    Outcome<void> getUntil(std::chrono::steady_clock::time_point const deadline) const {
        if (not isReady()) {
            detail::Waiter waiter;
            detail::DetachOnExit<void> const detach{*state, waiter};
            while (state->attach(waiter)) {
                if (not waiter.blockUntil(deadline)) {
                    break;
                }
            }
            if (not isReady()) {
                Outcome<void> timedOut;
                timedOut.error = std::make_error_code(std::errc::timed_out);
                return timedOut;
            }
        }
        return state->outcome;
    }

    template <class Rep, class Period>
    Outcome<void> getFor(std::chrono::duration<Rep, Period> const timeout) const {
        return getUntil(std::chrono::steady_clock::now() +
                        std::chrono::duration_cast<std::chrono::nanoseconds>(timeout));
    }

    void *toRaw() const { return new Completion(*this); }
    static Completion fromRaw(void *const raw) {
        std::unique_ptr<Completion> const owned(static_cast<Completion *>(raw));
        return *owned;
    }

    bool attach(detail::Waiter &waiter) const noexcept { return state->attach(waiter); }
    void detach(detail::Waiter &waiter) const noexcept { state->detach(waiter); }

private:
    std::shared_ptr<detail::CompletionState<void>> state;
};

// ---------------------------------------------------------------------------
// completionFor<Signature>(): the Completion whose T matches a callback
// signature — no arguments -> void, one -> that type, several -> std::tuple.
// Accepts R(Args...), R(*)(Args...) and std::function<R(Args...)>.
// ---------------------------------------------------------------------------
namespace detail {

template <class... Args>
struct PackToValue {
    using type = std::tuple<Args...>;
};
template <>
struct PackToValue<> {
    using type = void;
};
template <class Arg>
struct PackToValue<Arg> {
    using type = Arg;
};

template <class Signature>
struct CallbackValue;
template <class R, class... Args>
struct CallbackValue<R(Args...)> {
    using type = typename PackToValue<typename std::decay<Args>::type...>::type;
};
template <class R, class... Args>
struct CallbackValue<R (*)(Args...)> : CallbackValue<R(Args...)> {};
template <class R, class... Args>
struct CallbackValue<std::function<R(Args...)>> : CallbackValue<R(Args...)> {};

} // namespace detail

template <class Signature>
using CompletionFor = Completion<typename detail::CallbackValue<Signature>::type>;

template <class Signature>
CompletionFor<Signature> completionFor() {
    return CompletionFor<Signature>{};
}

} // namespace sync
} // namespace stackfull
