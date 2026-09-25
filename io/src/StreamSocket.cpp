#include <stackfull/io/StreamSocket.h>

#include "Socket.h"

#include <cstring>
#include <utility>

#include <sys/socket.h>

namespace stackfull {
namespace io {

// --- StreamSocket ------------------------------------------------------------------

StreamSocket::StreamSocket(Poller &poller, Fd socket_) noexcept
    : socket(std::move(socket_)), registration(poller, socket.get()) {
    registration.setKind(Registration::Kind::StreamSocket);
}

std::error_code StreamSocket::shutdownWrite() noexcept {
    if (::shutdown(socket.get(), SHUT_WR) != 0) {
        return lastError();
    }
    return std::error_code{};
}

SocketAddress StreamSocket::localAddress() const noexcept {
    return detail::localAddressOf(socket.get());
}

SocketAddress StreamSocket::peerAddress() const noexcept {
    return detail::peerAddressOf(socket.get());
}

// --- BufWriter -------------------------------------------------------------------

BufWriter::BufWriter(StreamSocket &stream_, std::size_t const capacity_)
    : stream(stream_), buffer(std::make_unique<char[]>(capacity_)), capacity(capacity_) {}

IoResult BufWriter::write(void const *const data, std::size_t const length) {
    if (used + length <= capacity) {
        std::memcpy(buffer.get() + used, data, length);
        used += length;
        return IoResult{length, std::error_code{}};
    }
    iovec vectors[2] = {{buffer.get(), used}, {const_cast<void *>(data), length}};
    IoResult const sent = stream.writevAll(vectors, 2);
    std::size_t const fromBuffer = sent.bytes < used ? sent.bytes : used;
    std::size_t const buffered = used;
    used = 0;
    if (not sent) {
        // Keep what the peer did not get from the buffer; report the payload unsent.
        std::memmove(buffer.get(), buffer.get() + fromBuffer, buffered - fromBuffer);
        used = buffered - fromBuffer;
        return IoResult{sent.bytes > buffered ? sent.bytes - buffered : 0, sent.error};
    }
    return IoResult{length, std::error_code{}};
}

IoResult BufWriter::flush() {
    if (used == 0) {
        return IoResult{0, std::error_code{}};
    }
    IoResult const sent = stream.writeAll(buffer.get(), used);
    std::memmove(buffer.get(), buffer.get() + sent.bytes, used - sent.bytes);
    used -= sent.bytes;
    return sent;
}

// --- BufReader -------------------------------------------------------------------

BufReader::BufReader(StreamSocket &stream_, std::size_t const capacity_)
    : stream(stream_), buffer(std::make_unique<char[]>(capacity_)), capacity(capacity_) {}

IoResult BufReader::readInto(void *const out, std::size_t const length, Deadline const deadline) {
    return deadline == Deadline::max() ? stream.read(out, length) : stream.readUntil(deadline, out, length);
}

IoResult BufReader::fill(Deadline const deadline) {
    if (begin != end) {
        return IoResult{end - begin, std::error_code{}};
    }
    begin = 0;
    end = 0;
    IoResult const got = readInto(buffer.get(), capacity, deadline);
    end = got.bytes;
    return got;
}

void BufReader::consume(std::size_t const count) noexcept {
    begin += count < end - begin ? count : end - begin;
}

IoResult BufReader::read(void *const out, std::size_t const length, Deadline const deadline) {
    if (begin == end and length >= capacity) {
        return readInto(out, length, deadline); // nothing to gain from copying through the buffer
    }
    IoResult const filled = fill(deadline);
    if (not filled or filled.bytes == 0) {
        return filled;
    }
    std::size_t const taken = length < buffered() ? length : buffered();
    std::memcpy(out, data(), taken);
    consume(taken);
    return IoResult{taken, std::error_code{}};
}

IoResult BufReader::readExactly(void *const out, std::size_t const length, Deadline const deadline) {
    std::size_t received = 0;
    while (received < length) {
        IoResult const chunk = read(static_cast<char *>(out) + received, length - received, deadline);
        if (not chunk) {
            return IoResult{received, chunk.error};
        }
        if (chunk.bytes == 0) {
            break;
        }
        received += chunk.bytes;
    }
    return IoResult{received, std::error_code{}};
}

IoResult BufReader::readLine(std::string &line, char const delimiter, std::size_t const limit,
                             Deadline const deadline) {
    std::size_t appended = 0;
    for (;;) {
        IoResult const filled = fill(deadline);
        if (not filled) {
            return IoResult{appended, filled.error};
        }
        if (filled.bytes == 0) {
            return IoResult{appended, std::error_code{}}; // end of stream
        }
        std::size_t const room = limit - appended;
        std::size_t const window = buffered() < room ? buffered() : room;
        auto const *const found = static_cast<char const *>(std::memchr(data(), delimiter, window));
        std::size_t const taken = found != nullptr ? static_cast<std::size_t>(found - data()) + 1 : window;
        line.append(data(), taken);
        consume(taken);
        appended += taken;
        if (found != nullptr) {
            return IoResult{appended, std::error_code{}};
        }
        if (appended == limit) {
            return IoResult{appended, std::make_error_code(std::errc::message_size)};
        }
    }
}

} // namespace io
} // namespace stackfull
