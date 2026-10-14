#pragma once

#include <stackfull/exec/detail/waiting_operation.hpp>

#include <lexec/core/completion_signatures.hpp>
#include <lexec/core/completion_tags.hpp>
#include <lexec/core/sender.hpp>

#include <stackfull/io/Async.h>
#include <stackfull/io/Poller.h>
#include <stackfull/io/Registration.h>
#include <stackfull/io/SocketAddress.h>
#include <stackfull/io/StreamSocket.h>
#include <stackfull/io/TcpStream.h>
#include <stackfull/io/UdpSocket.h>
#include <stackfull/io/UnixStream.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>

// Senders for IO on registered descriptors, waiting without a task: the
// readiness protocol of io::callWhenReady with a callback where a task
// would park. A transfer that can go ahead completes inline on the starting
// thread; one that waits completes on the thread polling the descriptor's
// Poller — a scheduler worker, outside any task — so what follows runs
// there unless moved with continues_on(sf). Stop requests take the wait back
// and complete with set_stopped; bytes already moved by write_all /
// read_exactly are then not reported. The socket (listener, poller) must
// outlive the operation.

namespace stackfull::exec {

namespace detail {

using io_signatures = lexec::completion_signatures<lexec::set_value_t(std::size_t), lexec::set_error_t(std::error_code),
                                                   lexec::set_stopped_t()>;

// Step: what to do between waits. run(op, seen) makes system calls until it
// completed `op` (true) or one would block (false), leaving in `seen` the
// event count to wait past.
template <class Step, class Rcvr>
struct io_operation : waiting_operation<io_operation<Step, Rcvr>, Rcvr> {
    using base = waiting_operation<io_operation, Rcvr>;

    io_operation(Step const &step_, Rcvr &&rcvr_) noexcept(std::is_nothrow_move_constructible_v<Rcvr>)
        : base(static_cast<Rcvr &&>(rcvr_)), step(step_) {}

    bool try_now() noexcept {
        if (std::error_code const error = step.events().error()) {
            this->finish_error(error);
            return true;
        }
        if (step.run(*this, seen)) {
            return true;
        }
        blocked = true; // drive() goes straight to the wait
        return false;
    }

    void drive() noexcept {
        for (;;) {
            if (not blocked and step.run(*this, seen)) {
                return;
            }
            blocked = false;
            switch (step.events().watch(Step::direction, seen, this->waiter)) {
            case sync::detail::EnqueueResult::Ready:
                continue;
            case sync::detail::EnqueueResult::Enqueued:
                return;
            case sync::detail::EnqueueResult::Cancelled:
                this->finish_stopped();
                return;
            }
        }
    }

    bool cancel_wait() noexcept { return step.events().unwatch(Step::direction, this->waiter); }

    Step step;
    std::uint32_t seen = 0;
    bool blocked = false;
};

template <class Step>
struct io_sender {
    using sender_concept = lexec::sender_t;
    using completion_signatures = typename Step::completion_signatures;

    template <class Rcvr>
    io_operation<Step, Rcvr> connect(Rcvr rcvr) const noexcept(std::is_nothrow_move_constructible_v<Rcvr>) {
        return {step, static_cast<Rcvr &&>(rcvr)};
    }

    Step step;
};

// Completes `op` from one attempt: true unless it would block.
template <class Op>
bool finish_transfer(Op &op, io::IoAttempt const &attempt) noexcept {
    if (attempt.wouldBlock) {
        return false;
    }
    if (attempt.error) {
        op.finish_error(attempt.error);
    } else {
        op.finish_value(attempt.bytes);
    }
    return true;
}

struct read_step {
    using completion_signatures = io_signatures;
    static constexpr io::Interest direction = io::Interest::Readable;

    io::Registration &events() const noexcept { return *registration; }
    template <class Op>
    bool run(Op &op, std::uint32_t &seen) noexcept {
        seen = registration->readEvents();
        return finish_transfer(op, io::tryRead(*registration, seen, buffer, length));
    }

    io::Registration *registration;
    void *buffer;
    std::size_t length;
};

struct write_step {
    using completion_signatures = io_signatures;
    static constexpr io::Interest direction = io::Interest::Writable;

    io::Registration &events() const noexcept { return *registration; }
    template <class Op>
    bool run(Op &op, std::uint32_t &seen) noexcept {
        seen = registration->writeEvents();
        return finish_transfer(op, io::tryWrite(*registration, seen, buffer, length));
    }

    io::Registration *registration;
    void const *buffer;
    std::size_t length;
};

// set_value(bytes): `length`, or fewer if the stream ended first.
struct read_exactly_step {
    using completion_signatures = io_signatures;
    static constexpr io::Interest direction = io::Interest::Readable;

    io::Registration &events() const noexcept { return *registration; }
    template <class Op>
    bool run(Op &op, std::uint32_t &seen) noexcept {
        while (received < length) {
            seen = registration->readEvents();
            io::IoAttempt const attempt = io::tryRead(*registration, seen, buffer + received, length - received);
            if (attempt.wouldBlock) {
                return false;
            }
            if (attempt.error) {
                op.finish_error(attempt.error);
                return true;
            }
            if (attempt.bytes == 0) {
                break; // end of stream
            }
            received += attempt.bytes;
        }
        op.finish_value(received);
        return true;
    }

    io::Registration *registration;
    char *buffer;
    std::size_t length;
    std::size_t received = 0;
};

struct write_all_step {
    using completion_signatures = io_signatures;
    static constexpr io::Interest direction = io::Interest::Writable;

    io::Registration &events() const noexcept { return *registration; }
    template <class Op>
    bool run(Op &op, std::uint32_t &seen) noexcept {
        while (sent < length) {
            seen = registration->writeEvents();
            io::IoAttempt const attempt = io::tryWrite(*registration, seen, buffer + sent, length - sent);
            if (attempt.wouldBlock) {
                return false;
            }
            if (attempt.error) {
                op.finish_error(attempt.error);
                return true;
            }
            sent += attempt.bytes;
        }
        op.finish_value(sent);
        return true;
    }

    io::Registration *registration;
    char const *buffer;
    std::size_t length;
    std::size_t sent = 0;
};

template <class Listener, class Stream>
struct accept_step {
    using completion_signatures =
        lexec::completion_signatures<lexec::set_value_t(std::unique_ptr<Stream>), lexec::set_error_t(std::error_code),
                                     lexec::set_stopped_t()>;
    static constexpr io::Interest direction = io::Interest::Readable;

    io::Registration &events() const noexcept { return listener->events(); }
    template <class Op>
    bool run(Op &op, std::uint32_t &seen) noexcept {
        seen = listener->events().readEvents();
        bool wouldBlock = false;
        auto accepted = listener->tryAccept(wouldBlock);
        if (wouldBlock) {
            return false;
        }
        if (not accepted) {
            op.finish_error(accepted.error);
        } else {
            op.finish_value(std::move(accepted.stream));
        }
        return true;
    }

    Listener *listener;
};

struct udp_recv_step {
    using completion_signatures = io_signatures;
    static constexpr io::Interest direction = io::Interest::Readable;

    io::Registration &events() const noexcept { return socket->events(); }
    template <class Op>
    bool run(Op &op, std::uint32_t &seen) noexcept {
        seen = socket->events().readEvents();
        return finish_transfer(op, socket->tryRecv(seen, buffer, length, from));
    }

    io::UdpSocket *socket;
    void *buffer;
    std::size_t length;
    io::SocketAddress *from;
};

struct udp_send_step {
    using completion_signatures = io_signatures;
    static constexpr io::Interest direction = io::Interest::Writable;

    io::Registration &events() const noexcept { return socket->events(); }
    template <class Op>
    bool run(Op &op, std::uint32_t &seen) noexcept {
        seen = socket->events().writeEvents();
        return finish_transfer(op, socket->trySend(seen, data, length, to));
    }

    io::UdpSocket *socket;
    void const *data;
    std::size_t length;
    io::SocketAddress const *to;
};

// The socket is registered only once the connect has started; its first
// write report then ends the handshake (see io::startConnect).
template <class Stream, class Rcvr>
struct connect_operation : waiting_operation<connect_operation<Stream, Rcvr>, Rcvr> {
    using base = waiting_operation<connect_operation, Rcvr>;

    connect_operation(io::Poller *const poller_, io::SocketAddress const &address_, Rcvr &&rcvr_) noexcept(
        std::is_nothrow_move_constructible_v<Rcvr>)
        : base(static_cast<Rcvr &&>(rcvr_)), poller(poller_), address(address_) {}

    bool try_now() noexcept {
        bool inProgress = false;
        auto started = Stream::startConnect(*poller, address, inProgress);
        if (not started) {
            this->finish_error(started.error);
            return true;
        }
        stream = std::move(started.stream);
        if (not inProgress) {
            this->finish_value(std::move(stream));
            return true;
        }
        return false;
    }

    void drive() noexcept {
        this->settle(stream->events().watch(io::Interest::Writable, 0, this->waiter), [this] { finish(); });
    }
    void on_notified() noexcept { finish(); }
    bool cancel_wait() noexcept { return stream->events().unwatch(io::Interest::Writable, this->waiter); }

    void finish() noexcept {
        if (std::error_code const error = io::finishConnect(stream->fd())) {
            stream.reset();
            this->finish_error(error);
        } else {
            this->finish_value(std::move(stream));
        }
    }

    io::Poller *poller;
    io::SocketAddress address;
    std::unique_ptr<Stream> stream;
};

template <class Stream>
struct connect_sender {
    using sender_concept = lexec::sender_t;
    using completion_signatures =
        lexec::completion_signatures<lexec::set_value_t(std::unique_ptr<Stream>), lexec::set_error_t(std::error_code),
                                     lexec::set_stopped_t()>;

    template <class Rcvr>
    connect_operation<Stream, Rcvr> connect(Rcvr rcvr) const noexcept(std::is_nothrow_move_constructible_v<Rcvr>) {
        return {poller, address, static_cast<Rcvr &&>(rcvr)};
    }

    io::Poller *poller;
    io::SocketAddress address;
};

} // namespace detail

// set_value(bytes) — 0: the peer closed — or set_error, or stopped. On any
// registered descriptor (Registration::setKind picks recv or read).
inline detail::io_sender<detail::read_step> async_read(io::Registration &events, void *const buffer,
                                                       std::size_t const length) noexcept {
    return {detail::read_step{&events, buffer, length}};
}
inline detail::io_sender<detail::read_step> async_read(io::StreamSocket &stream, void *const buffer,
                                                       std::size_t const length) noexcept {
    return async_read(stream.events(), buffer, length);
}

// set_value(bytes): `length`, fewer only if the stream ended first.
inline detail::io_sender<detail::read_exactly_step> async_read_exactly(io::Registration &events, void *const buffer,
                                                                       std::size_t const length) noexcept {
    return {detail::read_exactly_step{&events, static_cast<char *>(buffer), length}};
}
inline detail::io_sender<detail::read_exactly_step> async_read_exactly(io::StreamSocket &stream, void *const buffer,
                                                                       std::size_t const length) noexcept {
    return async_read_exactly(stream.events(), buffer, length);
}

// set_value(bytes the kernel took, possibly fewer than `length`).
inline detail::io_sender<detail::write_step> async_write(io::Registration &events, void const *const buffer,
                                                         std::size_t const length) noexcept {
    return {detail::write_step{&events, buffer, length}};
}
inline detail::io_sender<detail::write_step> async_write(io::StreamSocket &stream, void const *const buffer,
                                                         std::size_t const length) noexcept {
    return async_write(stream.events(), buffer, length);
}

// set_value(length) once every byte is sent.
inline detail::io_sender<detail::write_all_step> async_write_all(io::Registration &events, void const *const buffer,
                                                                 std::size_t const length) noexcept {
    return {detail::write_all_step{&events, static_cast<char const *>(buffer), length}};
}
inline detail::io_sender<detail::write_all_step> async_write_all(io::StreamSocket &stream, void const *const buffer,
                                                                 std::size_t const length) noexcept {
    return async_write_all(stream.events(), buffer, length);
}

// set_value(the accepted stream).
inline detail::io_sender<detail::accept_step<io::TcpListener, io::TcpStream>> async_accept(
    io::TcpListener &listener) noexcept {
    return {detail::accept_step<io::TcpListener, io::TcpStream>{&listener}};
}
inline detail::io_sender<detail::accept_step<io::UnixListener, io::UnixStream>> async_accept(
    io::UnixListener &listener) noexcept {
    return {detail::accept_step<io::UnixListener, io::UnixStream>{&listener}};
}

// set_value(the connected stream), registered with `poller`. Stream is
// io::TcpStream or io::UnixStream.
template <class Stream = io::TcpStream>
detail::connect_sender<Stream> async_connect(io::Poller &poller, io::SocketAddress const &address) noexcept {
    static_assert(std::is_same_v<Stream, io::TcpStream> or std::is_same_v<Stream, io::UnixStream>,
                  "stackfull::exec: async_connect makes a TcpStream or a UnixStream");
    return {&poller, address};
}

// One datagram: set_value(its size, at most `length`; the rest is lost).
inline detail::io_sender<detail::udp_recv_step> async_recv(io::UdpSocket &socket, void *const buffer,
                                                           std::size_t const length) noexcept {
    return {detail::udp_recv_step{&socket, buffer, length, nullptr}};
}
// Also tells who sent it; `from` must outlive the operation.
inline detail::io_sender<detail::udp_recv_step> async_recv_from(io::UdpSocket &socket, void *const buffer,
                                                                std::size_t const length,
                                                                io::SocketAddress &from) noexcept {
    return {detail::udp_recv_step{&socket, buffer, length, &from}};
}
// One datagram to the connected peer, all or nothing.
inline detail::io_sender<detail::udp_send_step> async_send(io::UdpSocket &socket, void const *const data,
                                                           std::size_t const length) noexcept {
    return {detail::udp_send_step{&socket, data, length, nullptr}};
}
// To `to`, which must outlive the operation.
inline detail::io_sender<detail::udp_send_step> async_send_to(io::UdpSocket &socket, void const *const data,
                                                              std::size_t const length,
                                                              io::SocketAddress const &to) noexcept {
    return {detail::udp_send_step{&socket, data, length, &to}};
}

} // namespace stackfull::exec
