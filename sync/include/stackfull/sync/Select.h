#pragma once

#include <stackfull/sync/detail/Waiter.h>

#include <chrono>
#include <cstddef>
#include <initializer_list>
#include <tuple>
#include <utility>

namespace stackfull {
namespace sync {

constexpr std::size_t kSelectTimedOut = static_cast<std::size_t>(-1);

// Waits until any of the sources is ready and returns its index (first
// ready wins on ties). A source is anything with
//   bool isReady() const; bool attach(detail::Waiter &); void detach(detail::Waiter &);
// — Completion, Stream, Mailbox. The same Waiter is attached to every
// source (as a Waker by value); whichever fires wakes it, and the
// attachments are cleared on the way out. Single consumer per source still applies.
//
// select() reports readiness only; call the source's tryNext()/get()
// afterwards (the loop handles the rare case where it was consumed meanwhile).

namespace detail {

template <class... Sources>
std::size_t firstReady(Sources &...sources) {
    bool const ready[] = {sources.isReady()...};
    for (std::size_t i = 0; i < sizeof...(Sources); ++i) {
        if (ready[i]) {
            return i;
        }
    }
    return kSelectTimedOut;
}

template <class... Sources>
struct DetachAllOnExit {
    Waiter &waiter;
    std::tuple<Sources &...> sources;
    ~DetachAllOnExit() { apply(std::index_sequence_for<Sources...>{}); }

    template <std::size_t... I>
    void apply(std::index_sequence<I...>) {
        std::initializer_list<int> const expand{(std::get<I>(sources).detach(waiter), 0)...};
        static_cast<void>(expand);
    }
};

template <class... Sources>
bool attachAll(Waiter &waiter, Sources &...sources) {
    bool const attached[] = {sources.attach(waiter)...};
    for (bool const a : attached) {
        if (not a) {
            return false; // one was already ready
        }
    }
    return true;
}

// hasDeadline == false waits indefinitely.
template <class... Sources>
std::size_t selectImpl(bool const hasDeadline, std::chrono::steady_clock::time_point const deadline,
                       Sources &...sources) {
    static_assert(sizeof...(Sources) >= 1, "select needs at least one source");
    for (;;) {
        std::size_t const ready = firstReady(sources...);
        if (ready != kSelectTimedOut) {
            return ready;
        }
        Waiter waiter;
        DetachAllOnExit<Sources...> const detach{waiter, std::tuple<Sources &...>(sources...)};
        if (not attachAll(waiter, sources...)) {
            continue; // became ready while attaching; re-scan
        }
        if (hasDeadline) {
            if (not waiter.blockUntil(deadline)) {
                return firstReady(sources...); // kSelectTimedOut unless something just arrived
            }
        } else {
            waiter.block(); // spurious wakeups loop back to the readiness scan
        }
    }
}

} // namespace detail

template <class... Sources>
std::size_t select(Sources &...sources) {
    return detail::selectImpl(false, std::chrono::steady_clock::time_point{}, sources...);
}

template <class... Sources>
std::size_t selectUntil(std::chrono::steady_clock::time_point const deadline, Sources &...sources) {
    return detail::selectImpl(true, deadline, sources...);
}

template <class Rep, class Period, class... Sources>
std::size_t selectFor(std::chrono::duration<Rep, Period> const timeout, Sources &...sources) {
    return detail::selectImpl(
        true, std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::nanoseconds>(timeout),
        sources...);
}

} // namespace sync
} // namespace stackfull
