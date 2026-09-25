#pragma once

#include <stackfull/io/Async.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>
#include <stackfull/io/SocketAddress.h>
#include <stackfull/io/StreamSocket.h>

#include <chrono>
#include <memory>
#include <system_error>
#include <utility>

namespace stackfull {
namespace io {

struct UnixStream;

struct UnixStreamResult {
    std::unique_ptr<UnixStream> stream;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

struct UnixStreamPair {
    std::unique_ptr<UnixStream> first;
    std::unique_ptr<UnixStream> second;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// A connected Unix-domain stream socket: local IPC by path or, on Linux and
// Android, by abstract name (SocketAddress::unixPath / unixAbstract).
// Reads, writes and timeouts: see StreamSocket.
struct UnixStream final : StreamSocket {
    // Fails with resource_unavailable_try_again when the listener's backlog
    // is full (the connection is not queued; try again later).
    static UnixStreamResult connect(Poller &poller, SocketAddress const &address,
                                    Deadline deadline = Deadline::max());
    // Two connected ends (socketpair), e.g. one for a child process.
    static UnixStreamPair pair(Poller &poller);
    // Takes over an already connected socket (made non-blocking here).
    static UnixStreamResult adopt(Poller &poller, Fd socket);

private:
    friend struct UnixListener;
    UnixStream(Poller &poller, Fd socket) noexcept : StreamSocket(poller, std::move(socket)) {}
    static UnixStreamResult wrap(Poller &poller, Fd socket);
};

struct UnixListener;

struct UnixListenerResult {
    std::unique_ptr<UnixListener> listener;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// A listening Unix-domain socket. A path-bound socket leaves its file
// behind when closed; remove a stale one before binding again (bind fails
// with address_in_use otherwise). Abstract names need no cleanup.
struct UnixListener {
    static UnixListenerResult bind(Poller &poller, SocketAddress const &address, int backlog = 128);

    UnixListener(UnixListener const &) = delete;
    UnixListener &operator=(UnixListener const &) = delete;
    ~UnixListener() = default;

    UnixStreamResult accept() { return acceptUntil(Deadline::max()); }
    UnixStreamResult acceptUntil(Deadline deadline);
    template <class Rep, class Period>
    UnixStreamResult acceptFor(std::chrono::duration<Rep, Period> const timeout) {
        return acceptUntil(std::chrono::steady_clock::now() +
                           std::chrono::duration_cast<std::chrono::nanoseconds>(timeout));
    }

    SocketAddress const &localAddress() const noexcept { return local; }
    int fd() const noexcept { return socket.get(); }

private:
    UnixListener(Poller &poller_, Fd socket_, SocketAddress const &local_) noexcept
        : poller(poller_), socket(std::move(socket_)), registration(poller_, socket.get()), local(local_) {}

    Poller &poller;
    Fd socket;
    Registration registration;
    SocketAddress local;
};

} // namespace io
} // namespace stackfull
