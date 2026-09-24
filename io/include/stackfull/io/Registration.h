#pragma once

#include <stackfull/io/Poller.h>
#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <system_error>

namespace stackfull {
namespace io {

// One descriptor's membership in a Poller plus the (at most one per
// direction) party waiting for it. Construct after the descriptor is
// non-blocking; destroy before closing it. The registration must outlive
// every wait on it. Waiters are kept as Wakers by value, so readiness is
// never delivered into a waiter's stack frame.
//
// Readiness is counted, not flagged: each report the poller delivers for a
// direction bumps that direction's event count. The protocol for an
// operation that may hit EAGAIN is
//
//     std::uint32_t const seen = registration.readEvents();
//     ... ::read() ... EAGAIN ...
//     registration.waitReadable(seen);   // returns at once if a report
//                                        // arrived since `seen`
//
// which stays correct with edge-triggered pollers: a report that lands
// between the system call and the wait is not lost.
//
// Waits work from tasks (park) and from plain threads (block), like the
// sync primitives.
struct Registration {
    using TimePoint = std::chrono::steady_clock::time_point;

    Registration(Poller &poller_, int fd_) noexcept;
    Registration(Registration const &) = delete;
    Registration &operator=(Registration const &) = delete;
    ~Registration();

    // Non-empty if add() failed; waits then return this error at once.
    std::error_code error() const noexcept { return addError; }
    int fd() const noexcept { return descriptor; }
    Poller &poller() const noexcept { return owner; }

    std::uint32_t readEvents() const noexcept { return read.events.load(std::memory_order_acquire); }
    std::uint32_t writeEvents() const noexcept { return write.events.load(std::memory_order_acquire); }

    // Return once a report newer than `seen` arrived for the direction.
    std::error_code waitReadable(std::uint32_t seen);
    std::error_code waitWritable(std::uint32_t seen);
    // Same, giving up at `deadline` with std::errc::timed_out.
    std::error_code waitReadableUntil(std::uint32_t seen, TimePoint deadline);
    std::error_code waitWritableUntil(std::uint32_t seen, TimePoint deadline);

    // Return once a report arrived that no earlier call of the same kind
    // consumed. Simple, but only safe with a single waiter per direction
    // that does not also use the counted form.
    std::error_code waitReadable();
    std::error_code waitWritable();

    // --- for Poller implementations ----------------------------------------
    // Record readiness and take the waiters to wake; call wake() after
    // releasing the poller's own locks.
    struct Wakeups {
        sync::detail::Waker reader;
        sync::detail::Waker writer;
        void wake() const noexcept {
            reader.wake();
            writer.wake();
        }
    };
    Wakeups collect(Interest ready) noexcept;
    void deliver(Interest ready) noexcept { collect(ready).wake(); }

private:
    struct Direction {
        std::atomic<std::uint32_t> events{0};
        std::uint32_t consumed = 0; // for the uncounted waits
        sync::detail::Waker waker;
    };

    std::error_code waitFor(Interest direction, std::uint32_t seen, bool hasDeadline, TimePoint deadline);
    Direction &slot(Interest const direction) noexcept { return direction == Interest::Readable ? read : write; }
    void detach(Interest direction, sync::detail::Waker const &waker) noexcept;

    Poller &owner;
    int const descriptor;
    std::error_code addError;

    sync::SpinLock lock;
    Direction read;
    Direction write;
};

} // namespace io
} // namespace stackfull
