#include <stackfull/io/Poller.h>

#include <stackfull/coro/Fatal.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Registration.h>
#include <stackfull/sync/SpinLock.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

namespace stackfull {
namespace io {

namespace {

// poll(2) backend: portable (QNX has no epoll), O(descriptors) per wait.
// Interest lives in a table guarded by a lock; wait() snapshots it into a
// pollfd array. arm() from another thread pokes the self-pipe so a wait
// already in progress re-reads the table instead of missing the new
// interest until its timeout.
struct PollPoller final : Poller {
    PollPoller() {
        int ends[2] = {-1, -1};
        STACKFULL_CHECK(::pipe(ends) == 0, "stackfull: pipe() failed");
        wakeRead = Fd{ends[0]};
        wakeWrite = Fd{ends[1]};
        setNonBlocking(wakeRead.get());
        setNonBlocking(wakeWrite.get());
    }

    std::error_code add(Registration &registration) noexcept override {
        int const fd = registration.fd();
        if (fd < 0) {
            return std::make_error_code(std::errc::bad_file_descriptor);
        }
        sync::SpinLockGuard const guard(lock);
        auto const index = static_cast<std::size_t>(fd);
        if (index >= table.size()) {
            table.resize(index + 1);
        }
        if (table[index].registration != nullptr) {
            return std::make_error_code(std::errc::file_exists);
        }
        table[index].registration = &registration;
        table[index].events = 0;
        return std::error_code{};
    }

    void remove(Registration &registration) noexcept override {
        sync::SpinLockGuard const guard(lock);
        auto const index = static_cast<std::size_t>(registration.fd());
        if (index < table.size() and table[index].registration == &registration) {
            table[index].registration = nullptr;
            table[index].events = 0;
        }
    }

    std::error_code arm(Registration &registration, Interest const interest) noexcept override {
        {
            sync::SpinLockGuard const guard(lock);
            auto const index = static_cast<std::size_t>(registration.fd());
            if (index >= table.size() or table[index].registration != &registration) {
                return std::make_error_code(std::errc::bad_file_descriptor);
            }
            short events = 0;
            if (has(interest, Interest::Readable)) {
                events |= POLLIN;
            }
            if (has(interest, Interest::Writable)) {
                events |= POLLOUT;
            }
            table[index].events = events;
        }
        wake(); // a blocked wait() must pick the new interest up
        return std::error_code{};
    }

    void wait(std::chrono::nanoseconds const timeout) override {
        int millis = -1;
        if (timeout >= std::chrono::nanoseconds{0}) {
            auto const ms = std::chrono::duration_cast<std::chrono::milliseconds>(timeout);
            millis = static_cast<int>(ms.count() + (ms < timeout ? 1 : 0));
        }

        snapshot.clear();
        snapshot.push_back(pollfd{wakeRead.get(), POLLIN, 0});
        {
            sync::SpinLockGuard const guard(lock);
            for (std::size_t fd = 0; fd < table.size(); ++fd) {
                if (table[fd].registration != nullptr and table[fd].events != 0) {
                    snapshot.push_back(pollfd{static_cast<int>(fd), table[fd].events, 0});
                }
            }
        }

        int const count = ::poll(snapshot.data(), static_cast<nfds_t>(snapshot.size()), millis);
        if (count <= 0) {
            return;
        }
        if ((snapshot[0].revents & POLLIN) != 0) {
            char drain[64];
            while (::read(wakeRead.get(), drain, sizeof drain) > 0) {
            }
        }
        for (std::size_t i = 1; i < snapshot.size(); ++i) {
            if (snapshot[i].revents != 0) {
                dispatch(snapshot[i]);
            }
        }
    }

    void wake() override {
        char const byte = 1;
        ssize_t const written = ::write(wakeWrite.get(), &byte, 1);
        static_cast<void>(written); // EAGAIN when the pipe is full: fine
    }

private:
    struct Entry {
        Registration *registration = nullptr;
        short events = 0;
    };

    void dispatch(pollfd const &item) noexcept {
        Interest ready = Interest::None;
        if ((item.revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)) != 0) {
            ready = ready | Interest::Readable;
        }
        if ((item.revents & (POLLOUT | POLLHUP | POLLERR | POLLNVAL)) != 0) {
            ready = ready | Interest::Writable;
        }
        sync::SpinLockGuard const guard(lock);
        auto const index = static_cast<std::size_t>(item.fd);
        if (index < table.size() and table[index].registration != nullptr) {
            // One-shot: stop watching until the next arm().
            short const watched = table[index].events;
            if (has(ready, Interest::Readable)) {
                table[index].events = static_cast<short>(table[index].events & ~POLLIN);
            }
            if (has(ready, Interest::Writable)) {
                table[index].events = static_cast<short>(table[index].events & ~POLLOUT);
            }
            if (watched != 0) {
                table[index].registration->deliver(ready);
            }
        }
    }

    Fd wakeRead;
    Fd wakeWrite;
    sync::SpinLock lock;
    std::vector<Entry> table;
    std::vector<pollfd> snapshot;
};

} // namespace

std::unique_ptr<Poller> makePollPoller() {
    return std::make_unique<PollPoller>();
}

std::unique_ptr<Poller> makeDefaultPoller() {
#if defined(__linux__)
    return makeEpollPoller();
#else
    return makePollPoller();
#endif
}

} // namespace io
} // namespace stackfull
