#pragma once

#include <stackfull/io/Async.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>
#include <stackfull/io/SocketAddress.h>
#include <stackfull/io/StreamSocket.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <system_error>
#include <utility>
#include <vector>

namespace stackfull {
namespace io {

struct TcpStream;

struct TcpStreamResult {
    std::unique_ptr<TcpStream> stream;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// A connected TCP socket (TCP_NODELAY on). Reads, writes and timeouts: see
// StreamSocket.
struct TcpStream final : StreamSocket {
    // Connects, parking until the handshake completes or `deadline` passes.
    static TcpStreamResult connect(Poller &poller, SocketAddress const &address,
                                   Deadline deadline = Deadline::max());
    // Tries the addresses in order (e.g. what io::resolve() returned) until
    // one connects; `deadline` bounds the whole attempt. The error is the
    // last address's.
    static TcpStreamResult connect(Poller &poller, std::vector<SocketAddress> const &addresses,
                                   Deadline deadline = Deadline::max());
    // Takes over an already connected socket (made non-blocking here).
    static TcpStreamResult adopt(Poller &poller, Fd socket);

    std::error_code setNoDelay(bool enabled) noexcept;

private:
    friend struct TcpListener;
    TcpStream(Poller &poller, Fd socket) noexcept : StreamSocket(poller, std::move(socket)) {}
    // `socket` is non-blocking already.
    static TcpStreamResult wrap(Poller &poller, Fd socket);
};

struct TcpListener;

struct TcpListenerResult {
    std::unique_ptr<TcpListener> listener;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

struct TcpListenOptions {
    int backlog = 128;
    // SO_REUSEPORT (Linux, Android): several listeners on one port, the
    // kernel spreading connections over them.
    bool reusePort = false;
};

// A listening TCP socket. Port 0 in the address picks a free port; port()
// reports it.
struct TcpListener {
    static TcpListenerResult bind(Poller &poller, SocketAddress const &address, int backlog = 128);
    static TcpListenerResult bind(Poller &poller, SocketAddress const &address, TcpListenOptions const &options);

    TcpListener(TcpListener const &) = delete;
    TcpListener &operator=(TcpListener const &) = delete;
    ~TcpListener() = default;

    TcpStreamResult accept() { return acceptUntil(Deadline::max()); }
    TcpStreamResult acceptUntil(Deadline deadline);
    template <class Rep, class Period>
    TcpStreamResult acceptFor(std::chrono::duration<Rep, Period> const timeout) {
        return acceptUntil(std::chrono::steady_clock::now() +
                           std::chrono::duration_cast<std::chrono::nanoseconds>(timeout));
    }

    SocketAddress const &localAddress() const noexcept { return local; }
    std::uint16_t port() const noexcept { return local.port(); }
    int fd() const noexcept { return socket.get(); }

private:
    TcpListener(Poller &poller_, Fd socket_, SocketAddress const &local_) noexcept
        : poller(poller_), socket(std::move(socket_)), registration(poller_, socket.get()), local(local_) {}

    Poller &poller;
    Fd socket;
    Registration registration;
    SocketAddress local;
};

} // namespace io
} // namespace stackfull
