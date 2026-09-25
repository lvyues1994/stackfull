#pragma once

#include <stackfull/io/Async.h>
#include <stackfull/io/Fd.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>
#include <stackfull/io/SocketAddress.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <system_error>

#include <sys/uio.h>

namespace stackfull {
namespace io {

// A connected stream socket (TCP, Unix domain) registered with a Poller:
// the part TcpStream and UnixStream share, and what protocol code can take
// to work over either. Every call works from a task (parks) or from a plain
// thread (blocks). The Poller must be one that a running scheduler polls,
// e.g. stackfull::defaultPoller() for defaultScheduler().
//
// Owns the descriptor and its Registration and tears them down in the right
// order. Not copyable or movable: the factories hand out unique_ptrs.
struct StreamSocket {
    StreamSocket(StreamSocket const &) = delete;
    StreamSocket &operator=(StreamSocket const &) = delete;
    virtual ~StreamSocket() = default;

    // bytes == 0 without error: the peer closed its side.
    IoResult read(void *buffer, std::size_t length) { return io::read(registration, buffer, length); }
    IoResult readUntil(Deadline const deadline, void *buffer, std::size_t length) {
        return io::readUntil(registration, buffer, length, deadline);
    }
    template <class Rep, class Period>
    IoResult readFor(std::chrono::duration<Rep, Period> const timeout, void *buffer, std::size_t length) {
        return readUntil(std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::nanoseconds>(timeout),
                         buffer, length);
    }
    // Fills `buffer` completely unless the peer closes first.
    IoResult readExactly(void *buffer, std::size_t length) { return io::readExactly(registration, buffer, length); }

    IoResult write(void const *buffer, std::size_t length) { return io::write(registration, buffer, length); }
    IoResult writeAll(void const *buffer, std::size_t length) { return io::writeAll(registration, buffer, length); }
    IoResult writeAllUntil(Deadline const deadline, void const *buffer, std::size_t length) {
        return io::writeAllUntil(registration, buffer, length, deadline);
    }
    // Sends every byte of every vector in as few system calls as it can.
    IoResult writevAll(iovec *vectors, int count) { return io::writevAll(registration, vectors, count); }

    std::error_code shutdownWrite() noexcept;

    SocketAddress localAddress() const noexcept;
    SocketAddress peerAddress() const noexcept;
    int fd() const noexcept { return socket.get(); }
    // For custom operations with the counted-wait protocol (see Registration
    // and io::callWhenReady).
    Registration &events() noexcept { return registration; }

protected:
    // Registers `socket_` (connected or connecting, non-blocking) with the
    // poller; check events().error() afterwards.
    StreamSocket(Poller &poller, Fd socket_) noexcept;

private:
    Fd socket;                 // declared first: closed last
    Registration registration; // leaves the poller before the socket closes
};

// Collects small writes and sends them together: one writev() per flush,
// or per write that does not fit (buffer and payload in the same call).
struct BufWriter {
    explicit BufWriter(StreamSocket &stream_, std::size_t capacity_ = 16 * 1024);
    BufWriter(BufWriter const &) = delete;
    BufWriter &operator=(BufWriter const &) = delete;

    // Everything is either buffered or sent on return.
    IoResult write(void const *data, std::size_t length);
    IoResult flush();
    std::size_t buffered() const noexcept { return used; }

private:
    StreamSocket &stream;
    std::unique_ptr<char[]> buffer;
    std::size_t const capacity;
    std::size_t used = 0;
};

// Reads through a buffer: one read() per refill, however small the pieces
// taken out of it — lines, length prefixes, headers. The deadline, where
// taken, bounds the whole call (Deadline::max(): none).
struct BufReader {
    explicit BufReader(StreamSocket &stream_, std::size_t capacity_ = 16 * 1024);
    BufReader(BufReader const &) = delete;
    BufReader &operator=(BufReader const &) = delete;

    // Up to `length` bytes: what is buffered, else one read (straight into
    // `out` when it is at least as large as the buffer). 0: end of stream.
    IoResult read(void *out, std::size_t length, Deadline deadline = Deadline::max());
    // `length` bytes unless the stream ends first (then bytes < length).
    IoResult readExactly(void *out, std::size_t length, Deadline deadline = Deadline::max());
    // Appends through the next `delimiter` (included) to `line`. At the end
    // of the stream, the unterminated rest if any; bytes == 0 when there is
    // none. std::errc::message_size once `limit` bytes pass without a
    // delimiter — the part read so far stays in `line`.
    IoResult readLine(std::string &line, char delimiter = '\n', std::size_t limit = 64 * 1024,
                      Deadline deadline = Deadline::max());

    // Reads if nothing is buffered; bytes = what is buffered afterwards
    // (0: end of stream). Then look at data()/buffered() and consume().
    IoResult fill(Deadline deadline = Deadline::max());
    char const *data() const noexcept { return buffer.get() + begin; }
    std::size_t buffered() const noexcept { return end - begin; }
    void consume(std::size_t count) noexcept;

private:
    IoResult readInto(void *out, std::size_t length, Deadline deadline);

    StreamSocket &stream;
    std::unique_ptr<char[]> buffer;
    std::size_t const capacity;
    std::size_t begin = 0;
    std::size_t end = 0;
};

} // namespace io
} // namespace stackfull
