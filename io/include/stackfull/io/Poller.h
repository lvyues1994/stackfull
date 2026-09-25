#pragma once

#include <stackfull/sched/Driver.h>

#include <cstdint>
#include <memory>
#include <system_error>

namespace stackfull {
namespace io {

struct Registration;

// Readiness directions. Bit flags.
enum class Interest : std::uint8_t {
    None = 0,
    Readable = 1,
    Writable = 2,
};

inline Interest operator|(Interest const a, Interest const b) noexcept {
    return static_cast<Interest>(static_cast<std::uint8_t>(a) | static_cast<std::uint8_t>(b));
}
inline bool has(Interest const set, Interest const bit) noexcept {
    return (static_cast<std::uint8_t>(set) & static_cast<std::uint8_t>(bit)) != 0;
}

// OS readiness multiplexer. Implements sched::Driver so the scheduler's
// timekeeper worker sleeps inside wait() and readiness turns straight into
// task wakeups; other threads may register descriptors concurrently.
//
// A poller reports readiness through Registration::collect(), which counts
// it; waiters compare counts (see Registration). Two ways to watch:
//   - edge-triggered (epoll): both directions are watched from add() on and
//     every edge is reported; arm() does nothing, so waits cost no system
//     call beyond the one that hit EAGAIN;
//   - one-shot (poll): arm() watches the given directions until the next
//     report for the descriptor.
struct Poller : sched::Driver {
    // Start tracking `registration.fd()`.
    virtual std::error_code add(Registration &registration) noexcept = 0;
    // Stop tracking; after this returns no further readiness is delivered.
    virtual void remove(Registration &registration) noexcept = 0;
    // Called before each wait with the directions someone waits on.
    virtual std::error_code arm(Registration &registration, Interest interest) noexcept = 0;
    // True if every arrival of data (or buffer space) is reported, even
    // while the previous report has not been acted on: edge triggering.
    virtual bool reportsEveryArrival() const noexcept { return false; }
};

#if defined(__linux__)
std::unique_ptr<Poller> makeEpollPoller(); // Linux, Android
#endif
std::unique_ptr<Poller> makePollPoller();    // poll(2): QNX and everything else
std::unique_ptr<Poller> makeDefaultPoller(); // epoll where available, else poll

} // namespace io
} // namespace stackfull
