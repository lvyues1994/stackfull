#pragma once

#include <stackfull/io/Poller.h>
#include <stackfull/sync/SpinLock.h>
#include <stackfull/sync/detail/Waiter.h>

#include <atomic>
#include <cstdint>
#include <system_error>

namespace stackfull {
namespace io {

// One descriptor's membership in a Poller plus the (at most one per
// direction) party waiting for it. Construct after the descriptor is
// non-blocking; destroy before closing it. The registration must outlive
// every wait on it.
//
// waitReadable()/waitWritable() work from tasks (park) and from plain
// threads (block), like the sync primitives.
struct Registration {
    Registration(Poller &poller_, int fd_) noexcept;
    Registration(Registration const &) = delete;
    Registration &operator=(Registration const &) = delete;
    ~Registration();

    // Non-empty if add() failed; waits then return this error at once.
    std::error_code error() const noexcept { return addError; }
    int fd() const noexcept { return descriptor; }
    Poller &poller() const noexcept { return owner; }

    // Return once the OS reported the direction ready (consumes that report;
    // a subsequent EAGAIN simply waits again). Errors come from arm().
    std::error_code waitReadable();
    std::error_code waitWritable();

    // --- for Poller implementations ----------------------------------------
    // Record readiness and wake whoever waits for those directions.
    void deliver(Interest ready) noexcept;

private:
    std::error_code waitFor(Interest direction);
    void detach(Interest direction, sync::detail::Waiter &waiter) noexcept;

    Poller &owner;
    int const descriptor;
    std::error_code addError;

    std::atomic<std::uint8_t> ready{0};
    sync::SpinLock lock;
    sync::detail::Waiter *readWaiter = nullptr;
    sync::detail::Waiter *writeWaiter = nullptr;
};

} // namespace io
} // namespace stackfull
