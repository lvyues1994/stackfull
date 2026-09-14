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
    for (;;) {
        // Consume a report that already arrived.
        std::uint8_t const pending = ready.load(std::memory_order_acquire);
        if ((pending & bits(direction)) != 0) {
            ready.fetch_and(static_cast<std::uint8_t>(~bits(direction)), std::memory_order_acq_rel);
            return std::error_code{};
        }

        sync::detail::Waiter waiter;
        Interest armed = Interest::None;
        {
            sync::SpinLockGuard const guard(lock);
            if (direction == Interest::Readable) {
                readWaiter = &waiter;
            } else {
                writeWaiter = &waiter;
            }
            // Arm for everything anyone currently waits on, so a concurrent
            // waiter in the other direction is not disarmed by our one-shot.
            armed = (readWaiter != nullptr ? Interest::Readable : Interest::None) |
                    (writeWaiter != nullptr ? Interest::Writable : Interest::None);
        }
        std::error_code const error = owner.arm(*this, armed);
        if (error) {
            detach(direction, waiter);
            return error;
        }
        // Forced unwind while parked: take our pointer back out of the slot,
        // or wait for a deliver() that already took it to finish with us.
        struct DetachOnUnwind {
            Registration &self;
            Interest direction;
            sync::detail::Waiter &waiter;
            bool armed = true;
            ~DetachOnUnwind() {
                if (armed) {
                    self.detach(direction, waiter);
                }
            }
        } detachOnUnwind{*this, direction, waiter};
        waiter.wait(); // deliver() unlinked us before notifying
        detachOnUnwind.armed = false;
    }
}

void Registration::detach(Interest const direction, sync::detail::Waiter &waiter) noexcept {
    bool stillLinked = false;
    {
        sync::SpinLockGuard const guard(lock);
        if (direction == Interest::Readable and readWaiter == &waiter) {
            readWaiter = nullptr;
            stillLinked = true;
        } else if (direction == Interest::Writable and writeWaiter == &waiter) {
            writeWaiter = nullptr;
            stillLinked = true;
        }
    }
    if (not stillLinked and not waiter.satisfied.load(std::memory_order_acquire)) {
        waiter.awaitNotifier(); // deliver() has our pointer and will store `satisfied`
    }
}

void Registration::deliver(Interest const readyNow) noexcept {
    sync::detail::Waiter *reader = nullptr;
    sync::detail::Waiter *writer = nullptr;
    {
        sync::SpinLockGuard const guard(lock);
        ready.fetch_or(bits(readyNow), std::memory_order_acq_rel);
        if (has(readyNow, Interest::Readable)) {
            reader = readWaiter;
            readWaiter = nullptr;
        }
        if (has(readyNow, Interest::Writable)) {
            writer = writeWaiter;
            writeWaiter = nullptr;
        }
    }
    if (reader != nullptr) {
        reader->notify();
    }
    if (writer != nullptr) {
        writer->notify();
    }
}

} // namespace io
} // namespace stackfull
