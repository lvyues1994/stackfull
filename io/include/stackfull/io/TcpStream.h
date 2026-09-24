#pragma once

#include <stackfull/io/Async.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <system_error>

#include <sys/socket.h>
#include <sys/uio.h>

namespace stackfull {
namespace io {

// An IPv4 or IPv6 endpoint.
struct SocketAddress {
    sockaddr_storage storage{};
    socklen_t length = 0;

    // Numeric host only ("127.0.0.1", "::1"); false if it does not parse.
    static bool parse(char const *host, std::uint16_t port, SocketAddress &out) noexcept;
    static SocketAddress loopback(std::uint16_t port) noexcept; // 127.0.0.1
    static SocketAddress any(std::uint16_t port) noexcept;      // 0.0.0.0

    sockaddr const *get() const noexcept { return reinterpret_cast<sockaddr const *>(&storage); }
    int family() const noexcept { return storage.ss_family; }
    std::uint16_t port() const noexcept;
};

struct TcpStream;

struct TcpStreamResult {
    std::unique_ptr<TcpStream> stream;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// A connected TCP socket registered with a Poller. Every call works from a
// task (parks) or from a plain thread (blocks). The Poller is passed in
// rather than looked up: it must be one that a running scheduler polls,
// e.g. stackfull::defaultPoller() for defaultScheduler().
//
// Owns the descriptor and its Registration and tears them down in the
// right order (registration first). Not copyable or movable: hold it by
// unique_ptr (as the factories return it) to pass it around.
struct TcpStream {
    // Connects, parking until the handshake completes or `deadline` passes.
    static TcpStreamResult connect(Poller &poller, SocketAddress const &address,
                                   Deadline deadline = Deadline::max());
    // Takes over an already connected socket (made non-blocking here).
    static TcpStreamResult adopt(Poller &poller, Fd socket);

    TcpStream(TcpStream const &) = delete;
    TcpStream &operator=(TcpStream const &) = delete;
    ~TcpStream() = default;

    // bytes == 0 without error: the peer closed its side.
    IoResult read(void *buffer, std::size_t length) { return io::read(*registration, buffer, length); }
    IoResult readUntil(Deadline const deadline, void *buffer, std::size_t length) {
        return io::readUntil(*registration, buffer, length, deadline);
    }
    template <class Rep, class Period>
    IoResult readFor(std::chrono::duration<Rep, Period> const timeout, void *buffer, std::size_t length) {
        return readUntil(std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::nanoseconds>(timeout),
                         buffer, length);
    }
    // Fills `buffer` completely unless the peer closes first.
    IoResult readExactly(void *buffer, std::size_t length) { return io::readExactly(*registration, buffer, length); }

    IoResult write(void const *buffer, std::size_t length) { return io::write(*registration, buffer, length); }
    IoResult writeAll(void const *buffer, std::size_t length) { return io::writeAll(*registration, buffer, length); }
    IoResult writeAllUntil(Deadline const deadline, void const *buffer, std::size_t length) {
        return io::writeAllUntil(*registration, buffer, length, deadline);
    }
    // Sends every byte of every vector in as few system calls as it can.
    IoResult writevAll(iovec *vectors, int count) { return io::writevAll(*registration, vectors, count); }

    std::error_code setNoDelay(bool enabled) noexcept;
    std::error_code shutdownWrite() noexcept;

    int fd() const noexcept { return socket.get(); }
    // For custom operations with the counted-wait protocol (see Registration).
    Registration &events() noexcept { return *registration; }

private:
    TcpStream(Fd socket_, std::unique_ptr<Registration> registration_) noexcept;

    Fd socket;                                  // declared first: closed last
    std::unique_ptr<Registration> registration; // leaves the poller before the socket closes
};

struct TcpListener;

struct TcpListenerResult {
    std::unique_ptr<TcpListener> listener;
    std::error_code error;

    explicit operator bool() const noexcept { return not error; }
};

// A listening TCP socket. Port 0 in the address picks a free port; port()
// reports it.
struct TcpListener {
    static TcpListenerResult bind(Poller &poller, SocketAddress const &address, int backlog = 128);

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
    TcpListener(Poller &poller_, Fd socket_, std::unique_ptr<Registration> registration_,
                SocketAddress const &local_) noexcept;

    Poller &poller;
    Fd socket;
    std::unique_ptr<Registration> registration;
    SocketAddress local;
};

// Collects small writes and sends them together: one writev() per flush,
// or per write that does not fit (buffer and payload in the same call).
struct BufWriter {
    explicit BufWriter(TcpStream &stream_, std::size_t capacity_ = 16 * 1024);
    BufWriter(BufWriter const &) = delete;
    BufWriter &operator=(BufWriter const &) = delete;

    // Everything is either buffered or sent on return.
    IoResult write(void const *data, std::size_t length);
    IoResult flush();
    std::size_t buffered() const noexcept { return used; }

private:
    TcpStream &stream;
    std::unique_ptr<char[]> buffer;
    std::size_t const capacity;
    std::size_t used = 0;
};

} // namespace io
} // namespace stackfull
