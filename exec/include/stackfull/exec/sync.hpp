#pragma once

#include <stackfull/exec/detail/waiting_operation.hpp>

#include <lexec/core/completion_signatures.hpp>
#include <lexec/core/completion_tags.hpp>
#include <lexec/core/sender.hpp>

#include <stackfull/coro/Config.h>
#include <stackfull/sync/Channel.h>
#include <stackfull/sync/Completion.h>
#include <stackfull/sync/Mutex.h>
#include <stackfull/sync/Semaphore.h>
#include <stackfull/sync/Stream.h>
#include <stackfull/sync/WaitGroup.h>

#include <optional>
#include <system_error>
#include <type_traits>
#include <utility>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

// Senders for the sync primitives, waiting without a task: they queue a
// callback where a task would park. Each completes inline on the thread that
// makes it possible — the starting thread if it need not wait, else the one
// that releases, sends, closes or requests stop — so what follows may run on
// any of those; continues_on(sf) moves it into a task. Stop requests take
// the operation out of the queue and complete it with set_stopped, unless it
// was satisfied first.

namespace stackfull::exec {

namespace detail {

using sync::detail::EnqueueResult;
using sync::detail::TransferResult;

// One-shot events: Source says whether it happened (taking it, for a
// permit), queues the waiter and delivers the completion.
template <class Source, class Rcvr>
struct event_operation : waiting_operation<event_operation<Source, Rcvr>, Rcvr> {
    using base = waiting_operation<event_operation, Rcvr>;

    event_operation(Source const &source_, Rcvr &&rcvr_) noexcept(std::is_nothrow_move_constructible_v<Rcvr>)
        : base(static_cast<Rcvr &&>(rcvr_)), source(source_) {}

    bool try_now() noexcept {
        if (not source.ready()) {
            return false;
        }
        source.complete(*this);
        return true;
    }
    void drive() noexcept {
        this->settle(source.enqueue(this->waiter), [this] { source.complete(*this); });
    }
    void on_notified() noexcept { source.complete(*this); }
    bool cancel_wait() noexcept { return source.cancel(this->waiter); }

    Source source;
};

template <class Source, class Signatures>
struct event_sender {
    using sender_concept = lexec::sender_t;
    using completion_signatures = Signatures;

    template <class Rcvr>
    event_operation<Source, Rcvr> connect(Rcvr rcvr) const noexcept(std::is_nothrow_move_constructible_v<Rcvr>) {
        return {source, static_cast<Rcvr &&>(rcvr)};
    }

    Source source;
};

using signal_signatures = lexec::completion_signatures<lexec::set_value_t(), lexec::set_stopped_t()>;

// The permit comes with the callback: release() takes it on the waiter's behalf.
struct semaphore_source {
    bool ready() noexcept { return semaphore->tryAcquire(); }
    EnqueueResult enqueue(sync::detail::Waiter &waiter) noexcept { return semaphore->acquireOrEnqueue(waiter); }
    bool cancel(sync::detail::Waiter &waiter) noexcept { return semaphore->cancel(waiter); }
    template <class Op>
    static void complete(Op &op) noexcept {
        op.finish_value();
    }

    sync::Semaphore *semaphore;
};

struct mutex_source {
    bool ready() noexcept { return mutex->tryLock(); }
    EnqueueResult enqueue(sync::detail::Waiter &waiter) noexcept { return mutex->lockOrEnqueue(waiter); }
    bool cancel(sync::detail::Waiter &waiter) noexcept { return mutex->cancel(waiter); }
    template <class Op>
    static void complete(Op &op) noexcept {
        op.finish_value();
    }

    sync::Mutex *mutex;
};

struct wait_group_source {
    bool ready() noexcept { return group->pendingCount() == 0; }
    EnqueueResult enqueue(sync::detail::Waiter &waiter) noexcept { return group->waitOrEnqueue(waiter); }
    bool cancel(sync::detail::Waiter &waiter) noexcept { return group->cancel(waiter); }
    template <class Op>
    static void complete(Op &op) noexcept {
        op.finish_value();
    }

    sync::WaitGroup *group;
};

template <class T>
struct completion_source {
    bool ready() noexcept { return completion.isReady(); }
    EnqueueResult enqueue(sync::detail::Waiter &waiter) noexcept { return completion.attachOrReady(waiter); }
    bool cancel(sync::detail::Waiter &waiter) noexcept { return completion.cancel(waiter); }
    // Ready by now, so get() does not wait.
    template <class Op>
    void complete(Op &op) noexcept {
        sync::Outcome<T> outcome = completion.get();
        if (outcome.error) {
            op.finish_error(outcome.error);
#if STACKFULL_HAS_EXCEPTIONS
        } else if (outcome.exception) {
            op.finish_error(std::move(outcome.exception));
#endif
        } else if constexpr (std::is_void_v<T>) {
            op.finish_value();
        } else {
            op.finish_value(std::move(outcome.value));
        }
    }

    sync::Completion<T> completion;
};

template <class T>
struct completion_value_signature {
    using type = lexec::set_value_t(T);
};

template <>
struct completion_value_signature<void> {
    using type = lexec::set_value_t();
};

#if STACKFULL_HAS_EXCEPTIONS
template <class T>
using completion_signatures_of =
    lexec::completion_signatures<typename completion_value_signature<T>::type, lexec::set_error_t(std::error_code),
                                 lexec::set_error_t(std::exception_ptr), lexec::set_stopped_t()>;
#else
template <class T>
using completion_signatures_of =
    lexec::completion_signatures<typename completion_value_signature<T>::type, lexec::set_error_t(std::error_code),
                                 lexec::set_stopped_t()>;
#endif

// Values move under the primitive's lock, inside noexcept completions.
template <class T>
inline constexpr bool movable_without_throwing_v = std::is_nothrow_move_constructible_v<T>;

// Channel and Stream: a notification means "try again".
template <class Transfer, class Rcvr>
struct transfer_operation : waiting_operation<transfer_operation<Transfer, Rcvr>, Rcvr> {
    using base = waiting_operation<transfer_operation, Rcvr>;

    transfer_operation(Transfer &&transfer_, Rcvr &&rcvr_) noexcept(
        std::is_nothrow_move_constructible_v<Transfer> and std::is_nothrow_move_constructible_v<Rcvr>)
        : base(static_cast<Rcvr &&>(rcvr_)), transfer(static_cast<Transfer &&>(transfer_)) {}

    bool try_now() noexcept { return finished(transfer.attempt(nullptr)); }
    void drive() noexcept { finished(transfer.attempt(&this->waiter)); }
    bool cancel_wait() noexcept { return transfer.cancel(this->waiter); }

    bool finished(TransferResult const result) noexcept {
        switch (result) {
        case TransferResult::Done:
            transfer.done(*this);
            return true;
        case TransferResult::Closed:
            transfer.closed(*this);
            return true;
        case TransferResult::Cancelled:
            this->finish_stopped();
            return true;
        case TransferResult::WouldBlock:
        case TransferResult::Enqueued:
            break;
        }
        return false;
    }

    Transfer transfer;
};

template <class Transfer>
struct transfer_sender {
    using sender_concept = lexec::sender_t;
    using completion_signatures = typename Transfer::completion_signatures;

    template <class Rcvr>
    transfer_operation<Transfer, Rcvr> connect(Rcvr rcvr) && noexcept(
        std::is_nothrow_move_constructible_v<Transfer> and std::is_nothrow_move_constructible_v<Rcvr>) {
        return {static_cast<Transfer &&>(transfer), static_cast<Rcvr &&>(rcvr)};
    }

    template <class Rcvr, class X = Transfer, std::enable_if_t<std::is_copy_constructible_v<X>, int> = 0>
    transfer_operation<Transfer, Rcvr> connect(Rcvr rcvr) const & {
        return {Transfer(transfer), static_cast<Rcvr &&>(rcvr)};
    }

    Transfer transfer;
};

template <class T>
struct channel_send {
    using completion_signatures = lexec::completion_signatures<lexec::set_value_t(bool), lexec::set_stopped_t()>;

    TransferResult attempt(sync::detail::Waiter *const waiter) noexcept { return channel->sendOrEnqueue(value, waiter); }
    bool cancel(sync::detail::Waiter &waiter) noexcept { return channel->cancelSend(waiter); }
    template <class Op>
    static void done(Op &op) noexcept {
        op.finish_value(true);
    }
    template <class Op>
    static void closed(Op &op) noexcept {
        op.finish_value(false);
    }

    sync::Channel<T> *channel;
    T value;
};

template <class T>
struct channel_recv {
    using completion_signatures =
        lexec::completion_signatures<lexec::set_value_t(std::optional<T>), lexec::set_stopped_t()>;

    TransferResult attempt(sync::detail::Waiter *const waiter) noexcept {
        return channel->recvOrEnqueue([this](T &&item) { received.emplace(static_cast<T &&>(item)); }, waiter);
    }
    bool cancel(sync::detail::Waiter &waiter) noexcept { return channel->cancelRecv(waiter); }
    template <class Op>
    void done(Op &op) noexcept {
        op.finish_value(std::move(received));
    }
    template <class Op>
    static void closed(Op &op) noexcept {
        op.finish_value(std::optional<T>{});
    }

    sync::Channel<T> *channel;
    std::optional<T> received;
};

template <class T>
struct stream_next {
    using completion_signatures =
        lexec::completion_signatures<lexec::set_value_t(std::optional<T>), lexec::set_stopped_t()>;

    TransferResult attempt(sync::detail::Waiter *const waiter) noexcept {
        return stream->nextOrAttach([this](T &&item) { received.emplace(static_cast<T &&>(item)); }, waiter);
    }
    bool cancel(sync::detail::Waiter &waiter) noexcept { return stream->cancel(waiter); }
    template <class Op>
    void done(Op &op) noexcept {
        op.finish_value(std::move(received));
    }
    template <class Op>
    static void closed(Op &op) noexcept {
        op.finish_value(std::optional<T>{});
    }

    sync::Stream<T> *stream;
    std::optional<T> received;
};

} // namespace detail

// Completes once the permit is taken (set_value()), or stopped.
inline detail::event_sender<detail::semaphore_source, detail::signal_signatures> async_acquire(
    sync::Semaphore &semaphore) noexcept {
    return {detail::semaphore_source{&semaphore}};
}

// Completes with the mutex held — unlock() it when done — or stopped.
inline detail::event_sender<detail::mutex_source, detail::signal_signatures> async_lock(sync::Mutex &mutex) noexcept {
    return {detail::mutex_source{&mutex}};
}

// Completes once the group's count is zero, or stopped.
inline detail::event_sender<detail::wait_group_source, detail::signal_signatures> async_wait(
    sync::WaitGroup &group) noexcept {
    return {detail::wait_group_source{&group}};
}

// Completes with what the Completion delivers: its value, set_error with its
// error_code (or exception), or stopped. Single consumer, like get().
template <class T>
detail::event_sender<detail::completion_source<T>, detail::completion_signatures_of<T>> async_get(
    sync::Completion<T> const &completion) {
    return {detail::completion_source<T>{completion}};
}

// set_value(true) once sent, set_value(false) if the channel is closed, or
// stopped (the value is then dropped with the operation).
template <class T, class U>
detail::transfer_sender<detail::channel_send<T>> async_send(sync::Channel<T> &channel, U &&value) {
    static_assert(detail::movable_without_throwing_v<T>, "stackfull::exec: Channel items must move without throwing");
    return {detail::channel_send<T>{&channel, T(static_cast<U &&>(value))}};
}

// set_value with the next item, or an empty optional once the channel is
// closed and drained; or stopped.
template <class T>
detail::transfer_sender<detail::channel_recv<T>> async_recv(sync::Channel<T> &channel) noexcept {
    static_assert(detail::movable_without_throwing_v<T>, "stackfull::exec: Channel items must move without throwing");
    return {detail::channel_recv<T>{&channel, std::nullopt}};
}

// The Stream's single consumer: set_value with the next item, or an empty
// optional once it is closed and drained; or stopped.
template <class T>
detail::transfer_sender<detail::stream_next<T>> async_next(sync::Stream<T> &stream) noexcept {
    static_assert(detail::movable_without_throwing_v<T>, "stackfull::exec: Stream items must move without throwing");
    return {detail::stream_next<T>{&stream, std::nullopt}};
}

} // namespace stackfull::exec
