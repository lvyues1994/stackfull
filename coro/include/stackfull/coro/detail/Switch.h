#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/coro/CoroutineState.h>
#include <stackfull/coro/Fatal.h>
#include <stackfull/coro/detail/ContextBlock.h>
#include <stackfull/coro/detail/EhGlobals.h>
#include <stackfull/coro/detail/Sanitizer.h>
#include <stackfull/coro/detail/ThreadState.h>
#include <stackfull/fcontext/Fcontext.h>

// The switch path. Everything here is inline, touches no TLS, takes no lock
// and makes no system call. The only indirect calls are the fcontext jump and
// an optional post-switch hook.
//
// Protocol: the departing context passes `&from` as the transfer data and has
// already written `from.thread->current = &to`. The arriving context therefore
// learns both who it is (`thread->current`) and which thread it is on
// (`prev.thread`) without any thread-local lookup — which is what makes
// resuming on a different thread safe.
//
// None of the functions that contain a jump call site are `noexcept`: a forced
// unwind is thrown at exactly that site (see Fcontext.h).

namespace stackfull {
namespace coro {
namespace detail {

inline void swapEhGlobals(ContextBlock &prev, ContextBlock &self) noexcept {
#if STACKFULL_SWAP_EH_GLOBALS
    EhGlobals &live = currentEhGlobals();
    prev.ehGlobals = live; // what the departing context left in this thread
    live = self.ehGlobals; // what we carried with us
#else
    static_cast<void>(prev);
    static_cast<void>(self);
#endif
}

// Bookkeeping shared by every way of arriving in a context: after a jump
// returns, inside an ontop function, and at first entry.
inline ThreadState &onArrival(ContextBlock &self, fcontext::transfer_t const transfer) noexcept {
    ContextBlock &prev = *static_cast<ContextBlock *>(transfer.data);
    prev.fctx = transfer.fctx;
    ThreadState &thread = *prev.thread;
    self.thread = &thread;
    swapEhGlobals(prev, self);
    asanFinishSwitch(self, prev);
    if (prev.postSwitch.fn != nullptr) {
        PostSwitchHook const hook = prev.postSwitch;
        prev.postSwitch = PostSwitchHook{};
        hook.fn(prev, hook.arg);
    }
    return thread;
}

inline void prepareDeparture(ContextBlock &from, ContextBlock &to, CoroutineState const fromState) noexcept {
    from.thread->current = &to;
    from.state = fromState;
    to.state = CoroutineState::Running;
}

// Suspend `from` — which must be the running context — and continue `to`.
// Returns once `from` is resumed, possibly on another thread; the returned
// ThreadState is the one `from` is running on *now*.
inline ThreadState &switchTo(ContextBlock &from, ContextBlock &to) {
    prepareDeparture(from, to, CoroutineState::Suspended);
    asanStartSwitch(from, to, /*fromWillResume=*/true);
    fcontext::transfer_t const transfer = fcontext::jump(to.fctx, &from);
    return onArrival(from, transfer);
}

// Like switchTo, but `fn` runs on `to`'s stack before `to` continues. `fn`
// receives the same transfer `to` would have and must call onArrival itself,
// because the code after `to`'s own jump does not run when `fn` throws.
inline ThreadState &switchToOnTop(ContextBlock &from, ContextBlock &to, fcontext::ontop_fn const fn) {
    prepareDeparture(from, to, CoroutineState::Suspended);
    asanStartSwitch(from, to, /*fromWillResume=*/true);
    fcontext::transfer_t const transfer = fcontext::ontop(to.fctx, &from, fn);
    return onArrival(from, transfer);
}

// Last departure of a finished context. Never returns.
[[noreturn]] inline void finishAndSwitchTo(ContextBlock &from, ContextBlock &to) {
    prepareDeparture(from, to, CoroutineState::Done);
    asanStartSwitch(from, to, /*fromWillResume=*/false);
    fcontext::jump(to.fctx, &from);
    fatal("stackfull: a finished coroutine was resumed");
}

} // namespace detail
} // namespace coro
} // namespace stackfull
