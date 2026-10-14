#pragma once

// Receivers and helpers shared by the exec tests.

#include <lexec/execution.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <optional>
#include <thread>
#include <type_traits>

namespace exec_probe {

namespace ex = lexec;

// How an operation completed. `last` holds the value it carried, as a long:
// a number, a bool, an optional's value (-2 when empty); stored before the
// count, so a reader that sees the count sees it.
struct record {
    std::atomic<int> values{0};
    std::atomic<int> errors{0};
    std::atomic<int> stops{0};
    std::atomic<long> last{-1};

    int completions() const noexcept { return values.load() + errors.load() + stops.load(); }
};

template <class V>
long as_long(V const &value) noexcept {
    if constexpr (std::is_arithmetic_v<V>) {
        return static_cast<long>(value);
    } else {
        return -1;
    }
}

template <class V>
long as_long(std::optional<V> const &value) noexcept {
    return value ? as_long(*value) : -2;
}

// Records into `log`; its stop token comes from `source` when one is given.
struct probe {
    using receiver_concept = ex::receiver_t;

    struct env {
        ex::inplace_stop_token token;
        ex::inplace_stop_token query(ex::get_stop_token_t) const noexcept { return token; }
    };

    template <class... Vs>
    void set_value(Vs &&...values) && noexcept {
        ((log->last.store(as_long(values))), ...);
        log->values.fetch_add(1);
    }
    template <class E>
    void set_error(E &&) && noexcept {
        log->errors.fetch_add(1);
    }
    void set_stopped() && noexcept { log->stops.fetch_add(1); }
    env get_env() const noexcept { return {source != nullptr ? source->get_token() : ex::inplace_stop_token{}}; }

    record *log;
    ex::inplace_stop_source *source = nullptr;
};

template <class Predicate>
bool waitFor(Predicate const &done, std::chrono::milliseconds const limit = std::chrono::milliseconds{10000}) {
    auto const giveUp = std::chrono::steady_clock::now() + limit;
    while (not done()) {
        if (std::chrono::steady_clock::now() > giveUp) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

// A few hundred iterations at most, varying with `seed`: spreads where two
// threads that left a barrier together interleave.
inline void spinFor(unsigned const seed) noexcept {
    unsigned const count = (seed * 2654435761u) >> 24;
    for (std::atomic<unsigned> i{0}; i.load(std::memory_order_relaxed) < count;) {
        i.fetch_add(1, std::memory_order_relaxed);
    }
}

// Each round starts an operation that has to wait, then a helper thread
// requests stop while this thread satisfies the wait — both leave a barrier
// together and spin a varying while first; the helper also waits up to
// `stopLag` more, varying, for waits satisfied through another thread (the
// poller, a timer). `check` gets the round's record once the operation
// completed and cleans up after it. Prints how often each side won, so a
// test whose race never happens is noticed.
template <class MakeSender, class Satisfy, class Check>
void raceStopAgainst(char const *const name, int const rounds, MakeSender const &make, Satisfy const &satisfy,
                     Check const &check, std::chrono::microseconds const stopLag = std::chrono::microseconds{0}) {
    std::atomic<ex::inplace_stop_source *> current{nullptr};
    std::atomic<int> go{0};
    std::atomic<int> ready{0};
    std::atomic<int> stopped{0};
    std::atomic<bool> quit{false};
    std::thread stopper([&] {
        int seen = 0;
        while (not quit.load(std::memory_order_acquire)) {
            int const round = go.load(std::memory_order_acquire);
            if (round == seen) {
                continue;
            }
            seen = round;
            ready.store(round, std::memory_order_release);
            spinFor(static_cast<unsigned>(round));
            if (stopLag.count() > 0) {
                auto const until = std::chrono::steady_clock::now() +
                                   std::chrono::microseconds{(round * 37) % (stopLag.count() + 1)};
                while (std::chrono::steady_clock::now() < until) {
                }
            }
            current.load()->request_stop();
            stopped.store(round, std::memory_order_release);
        }
    });
    int stopWins = 0;
    for (int round = 1; round <= rounds; ++round) {
        record log;
        ex::inplace_stop_source source;
        auto op = ex::connect(make(), probe{&log, &source});
        ex::start(op);
        current.store(&source);
        go.store(round, std::memory_order_release);
        while (ready.load(std::memory_order_acquire) != round) {
        }
        spinFor(static_cast<unsigned>(round) * 7u + 3u);
        satisfy();
        waitFor([&] { return stopped.load(std::memory_order_acquire) == round and log.completions() != 0; });
        stopWins += log.stops.load();
        check(log);
    }
    quit.store(true, std::memory_order_release);
    stopper.join();
    std::printf("%s: stop won %d of %d rounds\n", name, stopWins, rounds);
}

} // namespace exec_probe
