#pragma once

#include <chrono>

namespace stackfull {
namespace sched {

// External event source the scheduler sleeps on instead of a plain futex:
// the IO layer's Poller implements it. At any moment at most one worker —
// the "timekeeper" — is inside wait(); it also fires expired timers. Other
// idle workers sleep on their own Parker and are woken through the normal
// idle-worker notification.
//
// wait() must return after `timeout` (or sooner), or as soon as wake() is
// called from any thread, and must deliver readiness by waking tasks
// (WakeToken::wake()) before returning. It runs on a worker thread while
// that worker is idle, so it must not block for longer than asked. Tasks it
// wakes are placed as one batch when it returns (the calling worker runs
// the first), so it must also return promptly once it has delivered.
struct Driver {
    virtual ~Driver() = default;

    // Block for at most `timeout`; a negative timeout means "no timers, wait
    // until wake()". Dispatch whatever became ready.
    virtual void wait(std::chrono::nanoseconds timeout) = 0;

    // Interrupt a concurrent wait() (or make the next one return at once).
    virtual void wake() = 0;
};

} // namespace sched
} // namespace stackfull
