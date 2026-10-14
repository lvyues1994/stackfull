// Costs of the sender/receiver bridge (stackfull::exec).
//
//   stackfull_exec_bench [iterations]

#include <stackfull/exec/execution.hpp>

#include <lexec/execution.hpp>

#include <stackfull/io/Poller.h>
#include <stackfull/io/UnixStream.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/Channel.h>
#include <stackfull/sync/Mutex.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

namespace ex = lexec;
namespace exec = stackfull::exec;
namespace io = stackfull::io;
using namespace stackfull::sched;

namespace {

using Clock = std::chrono::steady_clock;

void report(char const *const name, Clock::duration const elapsed, long const ops) {
    auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    std::printf("%-58s %9.1f ns\n", name, static_cast<double>(ns) / static_cast<double>(ops));
}

std::unique_ptr<Scheduler> schedulerWith(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    return makeScheduler(options);
}

// Completes nothing but a counter; stoppable when given a source.
struct counting_receiver {
    using receiver_concept = ex::receiver_t;

    struct env {
        ex::inplace_stop_token token;
        ex::inplace_stop_token query(ex::get_stop_token_t) const noexcept { return token; }
    };

    template <class... Vs>
    void set_value(Vs &&...) && noexcept {
        ++*completions;
    }
    template <class E>
    void set_error(E &&) && noexcept {}
    void set_stopped() && noexcept { ++*completions; }
    env get_env() const noexcept { return {source != nullptr ? source->get_token() : ex::inplace_stop_token{}}; }

    long *completions;
    ex::inplace_stop_source *source = nullptr;
};

// Two tasks pass `iterations` items through a one-slot channel; the
// consumer receives with `recv`.
template <class Recv>
Clock::duration channelPingPong(long const iterations, Recv const &recv) {
    auto scheduler = schedulerWith(1);
    stackfull::sync::Channel<long> channel{1};
    std::atomic<int> finished{0};
    auto const start = Clock::now();
    scheduler->spawn([&] {
        for (long i = 0; i < iterations; ++i) {
            channel.send(i);
        }
        finished.fetch_add(1);
    });
    scheduler->spawn([&] {
        for (long i = 0; i < iterations; ++i) {
            recv(channel);
        }
        finished.fetch_add(1);
    });
    while (finished.load() != 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return Clock::now() - start;
}

// A one-byte request and its reply between two tasks over a socket pair,
// `iterations` times; `transfer(stream, byte, write)` moves one byte.
template <class Transfer>
Clock::duration socketPingPong(long const iterations, Transfer const &transfer) {
    auto poller = io::makeDefaultPoller();
    SchedulerOptions options;
    options.workers = 1;
    options.driver = poller.get();
    auto scheduler = makeScheduler(options);
    io::UnixStreamPair ends = io::UnixStream::pair(*poller);
    std::atomic<int> finished{0};
    auto const start = Clock::now();
    scheduler->spawn([&] {
        char byte = 'p';
        for (long i = 0; i < iterations; ++i) {
            transfer(*ends.first, byte, true);
            transfer(*ends.first, byte, false);
        }
        finished.fetch_add(1);
    });
    scheduler->spawn([&] {
        char byte = 0;
        for (long i = 0; i < iterations; ++i) {
            transfer(*ends.second, byte, false);
            transfer(*ends.second, byte, true);
        }
        finished.fetch_add(1);
    });
    while (finished.load() != 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    Clock::duration const elapsed = Clock::now() - start;
    scheduler.reset();
    return elapsed;
}

// Runs `body` in a task and returns how long it took.
template <class F>
Clock::duration timedInTask(Scheduler &scheduler, F body) {
    std::atomic<bool> done{false};
    Clock::duration elapsed{};
    scheduler.spawn([&] {
        auto const start = Clock::now();
        body();
        elapsed = Clock::now() - start;
        done.store(true);
    });
    while (not done.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return elapsed;
}

} // namespace

int main(int const argc, char **const argv) {
    long const iterations = argc > 1 ? std::atol(argv[1]) : 200'000L;
    std::printf("iterations: %ld\n", iterations);

    {
        auto scheduler = schedulerWith(1);
        report("wait(just(1)) in a task (completes inline)", timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       exec::wait(ex::just(1));
                   }
               }),
               iterations);
    }
    for (std::size_t const workers : {1u, 2u}) {
        auto scheduler = schedulerWith(workers);
        exec::scheduler const sf{*scheduler};
        char name[96];
        std::snprintf(name, sizeof name, "wait(schedule(own scheduler)) in a task, %zu workers", workers);
        report(name, timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       exec::wait(ex::schedule(sf));
                   }
               }),
               iterations);
        std::snprintf(name, sizeof name, "wait(exec::spawn(fn)) in a task, %zu workers", workers);
        report(name, timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       exec::wait(exec::spawn(sf, [] { return 1; }));
                   }
               }),
               iterations);
        std::snprintf(name, sizeof name, "  native: spawnJoinable + join in a task, %zu workers", workers);
        report(name, timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       scheduler->spawnJoinable([] {}).handle.join();
                   }
               }),
               iterations);
    }
    {
        auto scheduler = schedulerWith(1);
        ex::static_thread_pool pool{1};
        report("wait(schedule(lexec pool)) in a task: there and back", timedInTask(*scheduler, [&] {
                   for (long i = 0; i < iterations; ++i) {
                       exec::wait(ex::schedule(pool.get_scheduler()));
                   }
               }),
               iterations);
    }
    {
        auto scheduler = schedulerWith(1);
        exec::scheduler const sf{*scheduler};
        long const rounds = iterations / 10;
        auto const start = Clock::now();
        for (long i = 0; i < rounds; ++i) {
            ex::sync_wait(exec::spawn(sf, [] { return 1; }));
        }
        report("sync_wait(exec::spawn(fn)) from a plain thread", Clock::now() - start, rounds);
        ex::static_thread_pool pool{1};
        auto const poolStart = Clock::now();
        for (long i = 0; i < rounds; ++i) {
            ex::sync_wait(ex::schedule(pool.get_scheduler()));
        }
        report("  lexec only: sync_wait(schedule(pool)) from a plain thread", Clock::now() - poolStart, rounds);
    }

    // --- native senders ---------------------------------------------------------------
    {
        stackfull::sync::Mutex mutex;
        long completions = 0;
        ex::inplace_stop_source source;
        auto const start = Clock::now();
        for (long i = 0; i < iterations; ++i) {
            auto op = ex::connect(exec::async_lock(mutex), counting_receiver{&completions, &source});
            ex::start(op);
            mutex.unlock();
        }
        report("async_lock + unlock, free mutex, plain thread", Clock::now() - start, iterations);
        auto const nativeStart = Clock::now();
        for (long i = 0; i < iterations; ++i) {
            mutex.lock();
            mutex.unlock();
        }
        report("  native: Mutex::lock + unlock", Clock::now() - nativeStart, iterations);
    }
    report("channel ping-pong, 2 tasks, receiver: wait(async_recv)", channelPingPong(iterations, [](auto &channel) {
               exec::wait(exec::async_recv(channel));
           }),
           iterations);
    report("  native: receiver Channel::recv", channelPingPong(iterations, [](auto &channel) {
               long item = 0;
               channel.recv(item);
           }),
           iterations);
    {
        auto scheduler = schedulerWith(1);
        exec::scheduler const sf{*scheduler};
        long completions = 0;
        auto const start = Clock::now();
        for (long i = 0; i < iterations; ++i) {
            ex::inplace_stop_source source;
            auto op = ex::connect(exec::schedule_after(sf, std::chrono::seconds{10}), counting_receiver{&completions, &source});
            ex::start(op);
            source.request_stop();
        }
        report("schedule_after: start + stop (add, cancel a timer)", Clock::now() - start, iterations);
    }
    {
        long const rounds = iterations / 4;
        report("socket ping-pong, 2 tasks: wait(async_read / async_write)",
               socketPingPong(rounds,
                              [](io::StreamSocket &stream, char &byte, bool const write) {
                                  if (write) {
                                      exec::wait(exec::async_write(stream, &byte, 1));
                                  } else {
                                      exec::wait(exec::async_read(stream, &byte, 1));
                                  }
                              }),
               rounds);
        report("  native: StreamSocket::read / write",
               socketPingPong(rounds,
                              [](io::StreamSocket &stream, char &byte, bool const write) {
                                  if (write) {
                                      stream.write(&byte, 1);
                                  } else {
                                      stream.read(&byte, 1);
                                  }
                              }),
               rounds);
    }
    return 0;
}
