#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/ForcedUnwind.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Resolve.h>
#include <stackfull/io/TcpStream.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/Completion.h>

#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include <sys/socket.h>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

// Application-level entry points. Library code should take a Scheduler &
// (or a Completion / Stream) as a dependency instead of reaching for the
// process-wide default.

namespace stackfull {

// Lazily created on first use with hardware_concurrency() workers, already
// running, never destroyed. Its idle workers sleep in defaultPoller(), so
// tasks on it can do IO.
sched::Scheduler &defaultScheduler();

// The Poller that defaultScheduler() polls (created together with it). Pass
// it to io::TcpStream / io::TcpListener / io::Registration used from tasks
// on the default scheduler, or from plain threads.
io::Poller &defaultPoller();

// Fire-and-forget on the default scheduler.
template <class F>
sched::SpawnResult go(F &&body) {
    return defaultScheduler().spawn(std::forward<F>(body));
}

namespace detail {

template <class R, class F>
void runInto(sync::Completion<R> const &completion, F &body, std::false_type /*isVoid*/) {
    completion.set(body());
}

template <class R, class F>
void runInto(sync::Completion<R> const &completion, F &body, std::true_type /*isVoid*/) {
    body();
    completion.set();
}

template <class R, class F>
void runGuarded(sync::Completion<R> const &completion, F &body) {
#if STACKFULL_HAS_EXCEPTIONS
    try {
        runInto(completion, body, std::is_void<R>{});
    } catch (coro::ForcedUnwind const &) {
        throw; // shutdown unwinding must keep going
    } catch (...) {
        completion.setException(std::current_exception());
    }
#else
    runInto(completion, body, std::is_void<R>{});
#endif
}

} // namespace detail

// Spawn `body` and get its result as a Completion — the same handle used to
// await SDK callbacks, so "wait for a task" and "wait for a callback" read
// alike. An exception escaping the body arrives as Outcome::exception.
template <class F>
auto async(sched::Scheduler &scheduler, F &&body)
    -> sync::Completion<typename std::decay<decltype(body())>::type> {
    using R = typename std::decay<decltype(body())>::type;
    sync::Completion<R> completion;
    sched::SpawnResult const spawned =
        scheduler.spawn([completion, task = std::forward<F>(body)]() mutable { detail::runGuarded(completion, task); });
    if (not spawned) {
        completion.fail(spawned.error);
    }
    return completion;
}

template <class F>
auto async(F &&body) -> sync::Completion<typename std::decay<decltype(body())>::type> {
    return async(defaultScheduler(), std::forward<F>(body));
}

namespace detail {

template <class R>
R unwrap(sync::Outcome<R> &&outcome, std::false_type /*isVoid*/) {
#if STACKFULL_HAS_EXCEPTIONS
    if (outcome.exception) {
        std::rethrow_exception(outcome.exception);
    }
#endif
    return std::move(outcome.value);
}

template <class R>
void unwrap(sync::Outcome<R> &&outcome, std::true_type /*isVoid*/) {
#if STACKFULL_HAS_EXCEPTIONS
    if (outcome.exception) {
        std::rethrow_exception(outcome.exception);
    }
#else
    static_cast<void>(outcome);
#endif
}

} // namespace detail

// Run `body` as a task and block the calling thread until it returns; hands
// back its value (rethrowing its exception when exceptions are enabled).
// The shape of a main():  int main() { return blockOn([] { ...; return 0; }); }
template <class F>
auto blockOn(sched::Scheduler &scheduler, F &&body) -> typename std::decay<decltype(body())>::type {
    using R = typename std::decay<decltype(body())>::type;
    return detail::unwrap<R>(async(scheduler, std::forward<F>(body)).get(), std::is_void<R>{});
}

template <class F>
auto blockOn(F &&body) -> typename std::decay<decltype(body())>::type {
    return blockOn(defaultScheduler(), std::forward<F>(body));
}

namespace detail {

struct BlockingJob {
    virtual ~BlockingJob() = default;
    virtual void run() = 0;
};

template <class R, class F>
struct BlockingJobImpl final : BlockingJob {
    BlockingJobImpl(sync::Completion<R> completion_, F &&body_)
        : completion(std::move(completion_)), body(std::forward<F>(body_)) {}
    void run() override { runGuarded(completion, body); }

    sync::Completion<R> completion;
    typename std::decay<F>::type body;
};

// The blocking pool: threads created on demand (at most 64), idle ones exit
// after ten seconds. Never destroyed.
void submitBlocking(std::unique_ptr<BlockingJob> job);

} // namespace detail

// Runs `body` on a thread of the blocking pool and hands back its value
// (rethrowing its exception when exceptions are enabled). The calling task
// parks meanwhile, so its worker keeps running other tasks: wrap calls that
// block the thread, such as file IO, DNS lookups or synchronous SDK calls.
// From a plain thread, `body` simply runs inline.
template <class F>
auto blocking(F &&body) -> typename std::decay<decltype(body())>::type {
    using R = typename std::decay<decltype(body())>::type;
    if (not sched::this_task::isTask()) {
        return body();
    }
    sync::Completion<R> completion;
    detail::submitBlocking(std::unique_ptr<detail::BlockingJob>(
        new detail::BlockingJobImpl<R, F>(completion, std::forward<F>(body))));
    return detail::unwrap<R>(completion.get(), std::is_void<R>{});
}

// Name lookup without holding up the worker: io::resolve() on the blocking
// pool (inline for numeric hosts, and on plain threads).
io::ResolveResult resolve(char const *host, std::uint16_t port, int family = AF_UNSPEC);

// resolve(), then io::TcpStream::connect() on defaultPoller() over the
// addresses in order. `deadline` bounds the connecting, not the lookup.
io::TcpStreamResult connectTcp(char const *host, std::uint16_t port, io::Deadline deadline = io::Deadline::max());

} // namespace stackfull
