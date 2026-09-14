#include <stackfull/io/Poller.h>

#if defined(__linux__)

#include <stackfull/coro/Fatal.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Registration.h>
#include <stackfull/sync/SpinLock.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace stackfull {
namespace io {

namespace {

// epoll with one-shot interest. Events carry the descriptor number, not a
// pointer: the registration table is consulted under a lock at dispatch
// time, so a Registration destroyed between epoll_wait() returning and the
// event being processed is simply not found instead of being dereferenced.
struct EpollPoller final : Poller {
    EpollPoller() : epollFd(::epoll_create1(EPOLL_CLOEXEC)), wakeFd(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)) {
        STACKFULL_CHECK(epollFd and wakeFd, "stackfull: epoll_create1/eventfd failed");
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = wakeFd.get();
        STACKFULL_CHECK(::epoll_ctl(epollFd.get(), EPOLL_CTL_ADD, wakeFd.get(), &event) == 0,
                        "stackfull: epoll_ctl(ADD eventfd) failed");
    }

    std::error_code add(Registration &registration) noexcept override {
        int const fd = registration.fd();
        if (fd < 0) {
            return std::make_error_code(std::errc::bad_file_descriptor);
        }
        epoll_event event{};
        event.events = EPOLLONESHOT; // no interest until the first arm()
        event.data.fd = fd;
        {
            sync::SpinLockGuard const guard(tableLock);
            if (static_cast<std::size_t>(fd) >= table.size()) {
                table.resize(static_cast<std::size_t>(fd) + 1, nullptr);
            }
            if (table[static_cast<std::size_t>(fd)] != nullptr) {
                return std::make_error_code(std::errc::file_exists);
            }
            table[static_cast<std::size_t>(fd)] = &registration;
        }
        if (::epoll_ctl(epollFd.get(), EPOLL_CTL_ADD, fd, &event) != 0) {
            std::error_code const error = lastError();
            sync::SpinLockGuard const guard(tableLock);
            table[static_cast<std::size_t>(fd)] = nullptr;
            return error;
        }
        return std::error_code{};
    }

    void remove(Registration &registration) noexcept override {
        int const fd = registration.fd();
        {
            sync::SpinLockGuard const guard(tableLock);
            if (static_cast<std::size_t>(fd) < table.size() and table[static_cast<std::size_t>(fd)] == &registration) {
                table[static_cast<std::size_t>(fd)] = nullptr;
            }
        }
        ::epoll_ctl(epollFd.get(), EPOLL_CTL_DEL, fd, nullptr);
    }

    std::error_code arm(Registration &registration, Interest const interest) noexcept override {
        epoll_event event{};
        event.events = EPOLLONESHOT | EPOLLRDHUP;
        if (has(interest, Interest::Readable)) {
            event.events |= EPOLLIN;
        }
        if (has(interest, Interest::Writable)) {
            event.events |= EPOLLOUT;
        }
        event.data.fd = registration.fd();
        if (::epoll_ctl(epollFd.get(), EPOLL_CTL_MOD, registration.fd(), &event) != 0) {
            return lastError();
        }
        return std::error_code{};
    }

    void wait(std::chrono::nanoseconds const timeout) override {
        int millis = -1;
        if (timeout >= std::chrono::nanoseconds{0}) {
            // Round up so a timer is never fired early by the coarser clock.
            auto const ms = std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
            millis = static_cast<int>(ms.count() + (ms < timeout ? 1 : 0));
        }
        epoll_event events[kBatch];
        int const count = ::epoll_wait(epollFd.get(), events, kBatch, millis);
        for (int i = 0; i < count; ++i) {
            dispatch(events[i]);
        }
    }

    void wake() override {
        std::uint64_t const one = 1;
        ssize_t const written = ::write(wakeFd.get(), &one, sizeof one);
        static_cast<void>(written); // EAGAIN when already signalled: fine
    }

private:
    static constexpr int kBatch = 64;

    void dispatch(epoll_event const &event) noexcept {
        int const fd = event.data.fd;
        if (fd == wakeFd.get()) {
            std::uint64_t drained = 0;
            ssize_t const got = ::read(wakeFd.get(), &drained, sizeof drained);
            static_cast<void>(got);
            return;
        }
        Interest ready = Interest::None;
        if ((event.events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0) {
            ready = ready | Interest::Readable;
        }
        if ((event.events & (EPOLLOUT | EPOLLHUP | EPOLLERR)) != 0) {
            ready = ready | Interest::Writable;
        }
        // Deliver under the table lock: remove() nulls the slot under the
        // same lock, so a registration we find is alive for the whole call.
        sync::SpinLockGuard const guard(tableLock);
        if (static_cast<std::size_t>(fd) < table.size()) {
            if (Registration *const registration = table[static_cast<std::size_t>(fd)]) {
                registration->deliver(ready);
            }
        }
    }

    Fd epollFd;
    Fd wakeFd;
    sync::SpinLock tableLock;
    std::vector<Registration *> table;
};

} // namespace

std::unique_ptr<Poller> makeEpollPoller() {
    return std::make_unique<EpollPoller>();
}

} // namespace io
} // namespace stackfull

#endif // __linux__
