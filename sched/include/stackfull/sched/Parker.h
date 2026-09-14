#pragma once

#include <chrono>
#include <memory>

namespace stackfull {
namespace sched {

// Blocks one OS thread until another thread unparks it. Token semantics like
// std::thread park/unpark in Rust: an unpark() before park() makes that
// park() return immediately; multiple unparks coalesce.
//
// Backends: futex on Linux/Android, pthread condvar elsewhere (QNX). The IO
// layer will provide an epoll/poll based implementation of this same
// interface so idle waiting and IO waiting become one wait.
struct Parker {
    virtual ~Parker() = default;

    virtual void park() = 0;
    // False on timeout.
    virtual bool parkFor(std::chrono::nanoseconds timeout) = 0;
    virtual void unpark() = 0;
};

std::unique_ptr<Parker> makeParker();

} // namespace sched
} // namespace stackfull
