#include <stackfull/io/Registration.h>

#include <atomic>
#include <cstdint>

namespace stackfull {
namespace io {

namespace {

std::uint8_t bits(Interest const interest) noexcept {
    return static_cast<std::uint8_t>(interest);
}

} // namespace

Registration::Registration(Poller &poller_, int const fd_) noexcept : owner(poller_), descriptor(fd_) {
    addError = owner.add(*this);
}

Registration::~Registration() {
    if (not addError) {
        owner.remove(*this);
    }
}

std::error_code Registration::waitReadable() {
    return waitFor(Interest::Readable);
}

std::error_code Registration::waitWritable() {
    return waitFor(Interest::Writable);
}

std::error_code Registration::waitFor(Interest const direction) {
    if (addError) {
        return addError;
    }
    std::uint8_t const bit = bits(direction);
    sync::detail::Waiter waiter;
    sync::detail::Waker const me = waiter.waker();
    // Any way out, forced unwind included: take our waker back if deliver()
    // has not already. Nothing ever points into this frame.
    struct DetachOnExit {
        Registration &self;
        Interest direction;
        sync::detail::Waker const &me;
        ~DetachOnExit() { self.detach(direction, me); }
    } detachOnExit{*this, direction, me};
    for (;;) {
        // Consume a report that already arrived.
        if ((ready.load(std::memory_order_acquire) & bit) != 0) {
            ready.fetch_and(static_cast<std::uint8_t>(~bit), std::memory_order_acq_rel);
            return std::error_code{};
        }
        Interest armed = Interest::None;
        {
            sync::SpinLockGuard const guard(lock);
            (direction == Interest::Readable ? readWaker : writeWaker) = me;
            // Arm for everything anyone currently waits on, so a concurrent
            // waiter in the other direction is not disarmed by our one-shot.
            armed = (readWaker.isEmpty() ? Interest::None : Interest::Readable) |
                    (writeWaker.isEmpty() ? Interest::None : Interest::Writable);
        }
        if (std::error_code const error = owner.arm(*this, armed)) {
            return error;
        }
        // deliver() sets the bit before waking and wakeups are sticky, so a
        // report landing before we block is not lost.
        while ((ready.load(std::memory_order_acquire) & bit) == 0) {
            waiter.block();
        }
    }
}

void Registration::detach(Interest const direction, sync::detail::Waker const &waker) noexcept {
    sync::SpinLockGuard const guard(lock);
    sync::detail::Waker &slot = direction == Interest::Readable ? readWaker : writeWaker;
    if (slot == waker) {
        slot.clear();
    }
}

void Registration::deliver(Interest const readyNow) noexcept {
    sync::detail::Waker reader;
    sync::detail::Waker writer;
    {
        sync::SpinLockGuard const guard(lock);
        ready.fetch_or(bits(readyNow), std::memory_order_acq_rel);
        if (has(readyNow, Interest::Readable)) {
            reader = readWaker;
            readWaker.clear();
        }
        if (has(readyNow, Interest::Writable)) {
            writer = writeWaker;
            writeWaker.clear();
        }
    }
    reader.wake();
    writer.wake();
}

} // namespace io
} // namespace stackfull
