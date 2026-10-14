#include "ExecProbe.h"

#include <stackfull/exec/execution.hpp>

#include <lexec/execution.hpp>

#include <stackfull/coro/Config.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/Channel.h>
#include <stackfull/sync/Completion.h>
#include <stackfull/sync/Mutex.h>
#include <stackfull/sync/Semaphore.h>
#include <stackfull/sync/Stream.h>
#include <stackfull/sync/WaitGroup.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <system_error>
#include <thread>
#include <tuple>
#include <vector>

namespace ex = lexec;
namespace exec = stackfull::exec;
namespace sf_sync = stackfull::sync;
using namespace stackfull::sched;
using exec_probe::probe;
using exec_probe::raceStopAgainst;
using exec_probe::record;
using exec_probe::waitFor;
using std::chrono::milliseconds;

namespace {

std::unique_ptr<Scheduler> schedulerWith(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    return makeScheduler(options);
}

template <class Sndr>
auto valueOf(Sndr &&sndr) {
    auto result = ex::sync_wait(static_cast<Sndr &&>(sndr));
    EXPECT_TRUE(result.has_value());
    return std::get<0>(*result);
}

constexpr int kRaceRounds = 3000;

} // namespace

// --- Mutex / Semaphore ------------------------------------------------------------

TEST(ExecSync, AsyncLockCompletesAtOnceWhenFreeAndOnUnlockOtherwise) {
    sf_sync::Mutex mutex;
    record first;
    auto lock = ex::connect(exec::async_lock(mutex), probe{&first});
    ex::start(lock);
    EXPECT_EQ(first.values.load(), 1);

    record second;
    auto queued = ex::connect(exec::async_lock(mutex), probe{&second});
    ex::start(queued);
    EXPECT_EQ(second.completions(), 0);
    mutex.unlock();
    EXPECT_EQ(second.values.load(), 1); // inline, on the unlocking thread
    EXPECT_FALSE(mutex.tryLock());      // and it holds the mutex
    mutex.unlock();
}

TEST(ExecSync, AStopRequestTakesAQueuedLockBack) {
    sf_sync::Mutex mutex;
    mutex.lock();
    record log;
    ex::inplace_stop_source source;
    auto op = ex::connect(exec::async_lock(mutex), probe{&log, &source});
    ex::start(op);
    source.request_stop();
    EXPECT_EQ(log.stops.load(), 1);
    mutex.unlock();
    EXPECT_EQ(log.completions(), 1);
    EXPECT_TRUE(mutex.tryLock()); // the permit did not go to the cancelled waiter
    mutex.unlock();
}

TEST(ExecSync, StopRequestedBeforeStartTakesNothing) {
    sf_sync::Semaphore semaphore{1};
    record log;
    ex::inplace_stop_source source;
    source.request_stop();
    auto op = ex::connect(exec::async_acquire(semaphore), probe{&log, &source});
    ex::start(op);
    EXPECT_EQ(log.stops.load(), 1);
    EXPECT_EQ(semaphore.available(), 1u);
}

TEST(ExecSync, ReleaseHandsPermitsToQueuedAcquirersInOrder) {
    sf_sync::Semaphore semaphore{0};
    record a;
    record b;
    auto first = ex::connect(exec::async_acquire(semaphore), probe{&a});
    auto second = ex::connect(exec::async_acquire(semaphore), probe{&b});
    ex::start(first);
    ex::start(second);
    semaphore.release();
    EXPECT_EQ(a.values.load(), 1);
    EXPECT_EQ(b.completions(), 0);
    semaphore.release();
    EXPECT_EQ(b.values.load(), 1);
    EXPECT_EQ(semaphore.available(), 0u);
}

// A completion may release the semaphore again: notify runs outside its lock.
TEST(ExecSync, AContinuationMayReleaseTheSemaphoreItWasHandedFrom) {
    sf_sync::Semaphore semaphore{0};
    std::atomic<int> ran{0};
    record a;
    record b;
    auto one = ex::connect(exec::async_acquire(semaphore) | ex::then([&] {
                               ran.fetch_add(1);
                               semaphore.release();
                           }),
                           probe{&a});
    auto two = ex::connect(exec::async_acquire(semaphore), probe{&b});
    ex::start(one);
    ex::start(two);
    semaphore.release();
    EXPECT_EQ(ran.load(), 1);
    EXPECT_EQ(a.values.load(), 1);
    EXPECT_EQ(b.values.load(), 1);
}

TEST(ExecSync, StopRacingReleaseLosesNoPermit) {
    sf_sync::Semaphore semaphore{0};
    raceStopAgainst(
        "acquire", kRaceRounds, [&] { return exec::async_acquire(semaphore); }, [&] { semaphore.release(); },
        [&](record const &log) {
            ASSERT_EQ(log.completions(), 1);
            // A value holds the permit; a stop leaves it in the semaphore.
            EXPECT_EQ(semaphore.available(), log.stops.load() == 1 ? 1u : 0u);
            if (log.stops.load() == 1) {
                ASSERT_TRUE(semaphore.tryAcquire());
            }
        });
}

// --- WaitGroup ---------------------------------------------------------------------

TEST(ExecSync, AsyncWaitCompletesWhenTheGroupReachesZero) {
    sf_sync::WaitGroup group;
    EXPECT_TRUE(ex::sync_wait(exec::async_wait(group)));
    group.add(2);
    record log;
    auto op = ex::connect(exec::async_wait(group), probe{&log});
    ex::start(op);
    group.done();
    EXPECT_EQ(log.completions(), 0);
    std::thread([&] { group.done(); }).join();
    EXPECT_EQ(log.values.load(), 1);
}

TEST(ExecSync, StopRacingDoneCompletesOnce) {
    sf_sync::WaitGroup group;
    raceStopAgainst(
        "wait group", kRaceRounds,
        [&] {
            group.add(1);
            return exec::async_wait(group);
        },
        [&] { group.done(); }, [&](record const &log) { ASSERT_EQ(log.completions(), 1); });
}

// --- Channel -----------------------------------------------------------------------

TEST(ExecSync, ChannelSendAndRecv) {
    sf_sync::Channel<int> channel{1};
    record received;
    auto recv = ex::connect(exec::async_recv(channel), probe{&received});
    ex::start(recv);
    EXPECT_EQ(received.completions(), 0);
    EXPECT_TRUE(valueOf(exec::async_send(channel, 7)));
    EXPECT_EQ(received.last.load(), 7);

    EXPECT_TRUE(valueOf(exec::async_send(channel, 8))); // fills the one slot
    record sent;
    auto send = ex::connect(exec::async_send(channel, 9), probe{&sent});
    ex::start(send);
    EXPECT_EQ(sent.completions(), 0);
    EXPECT_EQ(valueOf(exec::async_recv(channel)), std::optional<int>{8});
    EXPECT_EQ(sent.values.load(), 1);
    EXPECT_EQ(sent.last.load(), 1); // true: sent

    channel.close();
    EXPECT_EQ(valueOf(exec::async_recv(channel)), std::optional<int>{9}); // drains
    EXPECT_EQ(valueOf(exec::async_recv(channel)), std::nullopt);
    EXPECT_FALSE(valueOf(exec::async_send(channel, 10)));
}

TEST(ExecSync, CloseCompletesQueuedSendersAndReceivers) {
    sf_sync::Channel<int> empty{1};
    record receiver;
    auto recv = ex::connect(exec::async_recv(empty), probe{&receiver});
    ex::start(recv);
    empty.close();
    EXPECT_EQ(receiver.last.load(), -2); // an empty optional

    sf_sync::Channel<int> full{1};
    ASSERT_TRUE(full.trySend(1));
    record sender;
    auto send = ex::connect(exec::async_send(full, 2), probe{&sender});
    ex::start(send);
    full.close();
    EXPECT_EQ(sender.last.load(), 0); // false: not sent
}

TEST(ExecSync, StoppedSendDropsItsValueAndLeavesTheChannelAlone) {
    sf_sync::Channel<std::unique_ptr<int>> channel{1};
    ASSERT_TRUE(channel.trySend(std::make_unique<int>(1)));
    record log;
    ex::inplace_stop_source source;
    auto op = ex::connect(exec::async_send(channel, std::make_unique<int>(2)), probe{&log, &source});
    ex::start(op);
    source.request_stop();
    EXPECT_EQ(log.stops.load(), 1);
    std::unique_ptr<int> out;
    ASSERT_TRUE(channel.tryRecv(out));
    EXPECT_EQ(*out, 1);
    EXPECT_FALSE(channel.tryRecv(out));
}

// Every value sent is either received or reported unsent: none is lost or
// delivered twice, whichever of send and stop wins.
TEST(ExecSync, StopRacingSendLosesNoValue) {
    sf_sync::Channel<int> channel{1};
    int next = 0;
    raceStopAgainst(
        "channel", kRaceRounds, [&] { return exec::async_recv(channel); },
        [&] { ASSERT_TRUE(channel.trySend(++next)); },
        [&](record const &log) {
            ASSERT_EQ(log.completions(), 1);
            if (log.values.load() == 1) {
                EXPECT_EQ(log.last.load(), next);
            } else {
                int left = 0;
                ASSERT_TRUE(channel.tryRecv(left));
                EXPECT_EQ(left, next);
            }
            EXPECT_EQ(channel.size(), 0u);
        });
}

TEST(ExecSync, ManyProducersAndConsumersThroughSenders) {
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 5000;
    sf_sync::Channel<int> channel{8};
    std::atomic<long> sum{0};
    std::atomic<int> received{0};
    std::vector<std::thread> threads;
    for (int p = 0; p < kProducers; ++p) {
        threads.emplace_back([&, p] {
            for (int i = 1; i <= kPerProducer; ++i) {
                ASSERT_TRUE(valueOf(exec::async_send(channel, p * kPerProducer + i)));
            }
        });
    }
    for (int c = 0; c < 2; ++c) {
        threads.emplace_back([&] {
            for (;;) {
                std::optional<int> const item = valueOf(exec::async_recv(channel));
                if (not item) {
                    return;
                }
                sum.fetch_add(*item);
                received.fetch_add(1);
            }
        });
    }
    for (int p = 0; p < kProducers; ++p) {
        threads[static_cast<std::size_t>(p)].join();
    }
    channel.close();
    for (std::size_t i = kProducers; i < threads.size(); ++i) {
        threads[i].join();
    }
    long const n = long{kProducers} * kPerProducer;
    EXPECT_EQ(received.load(), n);
    EXPECT_EQ(sum.load(), n * (n + 1) / 2);
}

// --- Stream ------------------------------------------------------------------------

TEST(ExecSync, StreamNextGetsWhatAnotherThreadPushes) {
    sf_sync::Stream<int> stream{4};
    record log;
    auto op = ex::connect(exec::async_next(stream), probe{&log});
    ex::start(op);
    EXPECT_EQ(log.completions(), 0);
    std::thread([&] { stream.push(5); }).join();
    EXPECT_EQ(log.last.load(), 5);
    stream.push(6);
    EXPECT_EQ(valueOf(exec::async_next(stream)), std::optional<int>{6});
    stream.close();
    EXPECT_EQ(valueOf(exec::async_next(stream)), std::nullopt);
}

TEST(ExecSync, StopRacingPushLosesNoItem) {
    sf_sync::Stream<int> stream{4};
    int next = 0;
    raceStopAgainst(
        "stream", kRaceRounds, [&] { return exec::async_next(stream); }, [&] { stream.push(++next); },
        [&](record const &log) {
            ASSERT_EQ(log.completions(), 1);
            if (log.values.load() == 1) {
                EXPECT_EQ(log.last.load(), next);
            } else {
                int left = 0;
                ASSERT_TRUE(stream.tryNext(left));
                EXPECT_EQ(left, next);
            }
            EXPECT_EQ(stream.size(), 0u);
        });
}

// --- Completion --------------------------------------------------------------------

TEST(ExecSync, AsyncGetDeliversValueErrorAndSignal) {
    sf_sync::Completion<int> value;
    std::thread setter([value] {
        std::this_thread::sleep_for(milliseconds{5});
        value.set(3);
    });
    EXPECT_EQ(valueOf(exec::async_get(value)), 3);
    setter.join();

    sf_sync::Completion<int> failing;
    record log;
    auto op = ex::connect(exec::async_get(failing), probe{&log});
    ex::start(op);
    failing.fail(std::make_error_code(std::errc::io_error));
    EXPECT_EQ(log.errors.load(), 1);

    sf_sync::Completion<void> signal;
    record signalled;
    auto wait = ex::connect(exec::async_get(signal), probe{&signalled});
    ex::start(wait);
    signal.set();
    EXPECT_EQ(signalled.values.load(), 1);
}

#if STACKFULL_HAS_EXCEPTIONS
TEST(ExecSync, AsyncGetCarriesTheExceptionThroughSyncWait) {
    sf_sync::Completion<int> completion;
    completion.setException(std::make_exception_ptr(std::runtime_error("boom")));
    EXPECT_THROW(ex::sync_wait(exec::async_get(completion)), std::runtime_error);
}
#endif

TEST(ExecSync, AStoppedGetLeavesTheOutcomeForTheNextOne) {
    sf_sync::Completion<int> completion;
    record log;
    ex::inplace_stop_source source;
    auto op = ex::connect(exec::async_get(completion), probe{&log, &source});
    ex::start(op);
    source.request_stop();
    EXPECT_EQ(log.stops.load(), 1);
    completion.set(4);
    EXPECT_EQ(log.completions(), 1);
    EXPECT_EQ(valueOf(exec::async_get(completion)), 4);
}

TEST(ExecSync, StopRacingSetCompletesOnce) {
    std::optional<sf_sync::Completion<int>> current;
    raceStopAgainst(
        "completion", kRaceRounds,
        [&] {
            current.emplace();
            return exec::async_get(*current);
        },
        [&] { current->set(1); },
        [&](record const &log) {
            ASSERT_EQ(log.completions(), 1);
            if (log.values.load() == 1) {
                EXPECT_EQ(log.last.load(), 1);
            }
        });
}

// --- timers ------------------------------------------------------------------------

TEST(ExecTimer, ScheduleAfterWaitsAndContinuesInATask) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    static_assert(ex::is_sender_v<decltype(exec::schedule_after(sf, milliseconds{1}))>);
    EXPECT_TRUE(ex::get_completion_scheduler<ex::set_value_t>(ex::get_env(exec::schedule_after(sf, milliseconds{1}))) ==
                sf);
    auto const before = std::chrono::steady_clock::now();
    bool const inTask = valueOf(exec::schedule_after(sf, milliseconds{20}) | ex::then([&] {
                                    return this_task::isTask() and &this_task::scheduler() == scheduler.get();
                                }));
    EXPECT_GE(std::chrono::steady_clock::now() - before, milliseconds{20});
    EXPECT_TRUE(inTask);
}

TEST(ExecTimer, ScheduleAtAPastDeadlineCompletesSoon) {
    auto scheduler = schedulerWith(1);
    exec::scheduler const sf{*scheduler};
    auto const before = std::chrono::steady_clock::now();
    EXPECT_TRUE(ex::sync_wait(exec::schedule_at(sf, before - milliseconds{5})));
    EXPECT_LT(std::chrono::steady_clock::now() - before, std::chrono::seconds{1});
}

TEST(ExecTimer, InsideATaskWaitSleepsLikeSleepFor) {
    auto scheduler = schedulerWith(1);
    exec::scheduler const sf{*scheduler};
    auto const before = std::chrono::steady_clock::now();
    auto const slept = ex::sync_wait(exec::spawn(sf, [&] {
        for (int i = 0; i < 5; ++i) {
            exec::wait(exec::schedule_after(sf, milliseconds{2}));
        }
        return std::chrono::steady_clock::now();
    }));
    ASSERT_TRUE(slept);
    EXPECT_GE(std::get<0>(*slept) - before, milliseconds{10});
}

TEST(ExecTimer, AStopRequestTakesAPendingTimerBack) {
    auto scheduler = schedulerWith(1);
    exec::scheduler const sf{*scheduler};
    record log;
    ex::inplace_stop_source source;
    auto op = ex::connect(exec::schedule_after(sf, std::chrono::seconds{30}), probe{&log, &source});
    ex::start(op);
    EXPECT_EQ(log.completions(), 0);
    source.request_stop();
    EXPECT_EQ(log.stops.load(), 1); // inline, at once
}

TEST(ExecTimer, SchedulerStopCallsPendingTimersBackWithStopped) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    record log;
    auto op = ex::connect(exec::schedule_after(sf, std::chrono::seconds{30}), probe{&log});
    ex::start(op);
    scheduler->stop();
    EXPECT_EQ(log.stops.load(), 1);
    record late;
    auto after = ex::connect(exec::schedule_after(sf, milliseconds{1}), probe{&late});
    ex::start(after);
    EXPECT_EQ(late.stops.load(), 1);
}

TEST(ExecTimer, WhenAnyPutsATimeoutOnAWait) {
    auto scheduler = schedulerWith(1);
    exec::scheduler const sf{*scheduler};
    sf_sync::Channel<int> channel{1};
    int const winner = valueOf(ex::when_any(exec::async_recv(channel) | ex::then([](std::optional<int> v) {
                                                return v ? *v : -1;
                                            }),
                                            exec::schedule_after(sf, milliseconds{10}) | ex::then([] { return -3; })));
    EXPECT_EQ(winner, -3);
    // The receive was taken back: a value sent now stays in the channel.
    EXPECT_TRUE(channel.trySend(1));
    EXPECT_EQ(channel.size(), 1u);
}

TEST(ExecTimer, ManyTimersFromSeveralThreadsEachCompleteOnce) {
    auto scheduler = schedulerWith(3);
    exec::scheduler const sf{*scheduler};
    constexpr int kThreads = 4;
    constexpr int kPerThread = 200;
    std::atomic<int> values{0};
    std::atomic<int> stops{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kPerThread; ++i) {
                record log;
                ex::inplace_stop_source source;
                auto op = ex::connect(exec::schedule_after(sf, std::chrono::microseconds{(i * 37 + t * 11) % 500}),
                                      probe{&log, &source});
                ex::start(op);
                if (i % 3 == 0) {
                    source.request_stop();
                }
                ASSERT_TRUE(waitFor([&] { return log.completions() != 0; }));
                values.fetch_add(log.values.load());
                stops.fetch_add(log.stops.load());
            }
        });
    }
    for (std::thread &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(values.load() + stops.load(), kThreads * kPerThread);
    EXPECT_GE(stops.load(), 1);
}

TEST(ExecTimer, StopRacingTheDeadlineCompletesOnce) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    raceStopAgainst(
        "timer", 1000, [&] { return exec::schedule_after(sf, std::chrono::microseconds{20}); }, [] {},
        [](record const &log) { ASSERT_EQ(log.completions(), 1); }, std::chrono::microseconds{200});
}
