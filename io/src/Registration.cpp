#include <stackfull/io/Registration.h>

#include <atomic>
#include <cstdint>

namespace stackfull {
namespace io {

Registration::Registration(Poller &poller_, int const fd_) noexcept
    : owner(poller_), descriptor(fd_), everyArrivalReported(poller_.reportsEveryArrival()) {
    addError = owner.add(*this);
}

Registration::~Registration() {
    if (not addError) {
        owner.remove(*this);
    }
}

std::error_code Registration::waitReadable(std::uint32_t const seen) {
    return waitFor(Interest::Readable, seen, false, TimePoint{});
}

std::error_code Registration::waitWritable(std::uint32_t const seen) {
    return waitFor(Interest::Writable, seen, false, TimePoint{});
}

std::error_code Registration::waitReadableUntil(std::uint32_t const seen, TimePoint const deadline) {
    return waitFor(Interest::Readable, seen, true, deadline);
}

std::error_code Registration::waitWritableUntil(std::uint32_t const seen, TimePoint const deadline) {
    return waitFor(Interest::Writable, seen, true, deadline);
}

std::error_code Registration::waitReadable() {
    std::error_code const error = waitFor(Interest::Readable, read.consumed, false, TimePoint{});
    read.consumed = readEvents();
    return error;
}

std::error_code Registration::waitWritable() {
    std::error_code const error = waitFor(Interest::Writable, write.consumed, false, TimePoint{});
    write.consumed = writeEvents();
    return error;
}

std::error_code Registration::waitFor(Interest const direction, std::uint32_t const seen, bool const hasDeadline,
                                      TimePoint const deadline) {
    if (addError) {
        return addError;
    }
    Direction &mine = slot(direction);
    if (mine.events.load(std::memory_order_acquire) != seen) {
        return std::error_code{};
    }
    sync::detail::Waiter waiter;
    sync::detail::Waker const me = waiter.waker();
    // Any way out, forced unwind and timeout included: take our waker back
    // if collect() has not already. Nothing ever points into this frame.
    struct DetachOnExit {
        Registration &self;
        Interest direction;
        sync::detail::Waker const &me;
        ~DetachOnExit() { self.detach(direction, me); }
    } detachOnExit{*this, direction, me};

    Interest armed = Interest::None;
    {
        sync::SpinLockGuard const guard(lock);
        mine.waker = me;
        // Pollers without edge triggering watch one-shot: arm for everything
        // anyone waits on, so the other direction's waiter is not disarmed.
        armed = (read.waker.isEmpty() ? Interest::None : Interest::Readable) |
                (write.waker.isEmpty() ? Interest::None : Interest::Writable);
    }
    if (std::error_code const error = owner.arm(*this, armed)) {
        return error;
    }
    // collect() bumps the count before waking and wakeups are sticky, so a
    // report landing before we block is not lost.
    while (mine.events.load(std::memory_order_acquire) == seen) {
        if (not hasDeadline) {
            waiter.block();
        } else if (not waiter.blockUntil(deadline)) {
            if (mine.events.load(std::memory_order_acquire) != seen) {
                break; // arrived just as we timed out
            }
            return std::make_error_code(std::errc::timed_out);
        }
    }
    return std::error_code{};
}

void Registration::detach(Interest const direction, sync::detail::Waker const &waker) noexcept {
    sync::SpinLockGuard const guard(lock);
    sync::detail::Waker &waiting = slot(direction).waker;
    if (waiting == waker) {
        waiting.clear();
    }
}

Registration::Wakeups Registration::collect(Interest const readyNow, Interest const closed) noexcept {
    Wakeups wakeups;
    sync::SpinLockGuard const guard(lock);
    if (has(closed, Interest::Readable)) {
        read.closed.store(true, std::memory_order_relaxed);
    }
    if (has(closed, Interest::Writable)) {
        write.closed.store(true, std::memory_order_relaxed);
    }
    if (has(readyNow, Interest::Readable)) {
        read.events.fetch_add(1, std::memory_order_acq_rel);
        wakeups.reader = read.waker;
        read.waker.clear();
    }
    if (has(readyNow, Interest::Writable)) {
        write.events.fetch_add(1, std::memory_order_acq_rel);
        wakeups.writer = write.waker;
        write.waker.clear();
    }
    return wakeups;
}

} // namespace io
} // namespace stackfull
