#pragma once

#include <lexec/detail/config.hpp>
#include <lexec/stop_token.hpp>

#include <stackfull/sched/TaskContext.h>
#include <stackfull/sched/ThisTask.h>

#include <exception>
#include <system_error>
#include <type_traits>
#include <utility>

namespace stackfull::exec {

#if LEXEC_HAS_EXCEPTIONS
// Thrown by exec::await when the awaited sender completes with set_stopped;
// a task started by exec::spawn that ends with it completes with set_stopped.
struct stopped_error : std::exception {
    char const *what() const noexcept override { return "stackfull::exec: the awaited sender completed with stopped"; }
};
#endif

namespace detail {

// What exec keeps for a task it started: the task's stop source, reached by
// the receiver's stop requests and handed to the senders the task waits for.
struct task_context : sched::TaskContext {
    lexec::inplace_stop_source *stop_source = nullptr;
};

// The calling task's stop source if exec::spawn started it, else null.
inline lexec::inplace_stop_source *task_stop_source() noexcept {
    sched::TaskContext *const context = sched::this_task::context();
    return context != nullptr ? static_cast<task_context *>(context)->stop_source : nullptr;
}

#if LEXEC_HAS_EXCEPTIONS
template <class E>
std::exception_ptr as_exception_ptr(E &&error) noexcept {
    using Error = std::decay_t<E>;
    if constexpr (std::is_same_v<Error, std::exception_ptr>) {
        return static_cast<E &&>(error);
    } else if constexpr (std::is_same_v<Error, std::error_code>) {
        return std::make_exception_ptr(std::system_error(error));
    } else {
        return std::make_exception_ptr(static_cast<E &&>(error));
    }
}
#endif

} // namespace detail

// The calling task's stop token: what the receiver of the exec::spawn that
// started it requests, and what Scheduler::stop() reaches through exec::wait.
// Empty in a task exec did not start.
inline lexec::inplace_stop_token get_stop_token() noexcept {
    lexec::inplace_stop_source *const source = detail::task_stop_source();
    return source != nullptr ? source->get_token() : lexec::inplace_stop_token{};
}

// For loops in a task to poll: its receiver asked it to stop, or its
// scheduler is stopping.
inline bool stop_requested() noexcept {
    return get_stop_token().stop_requested() or sched::this_task::stopRequested();
}

} // namespace stackfull::exec
