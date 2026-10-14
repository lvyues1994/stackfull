#include <stackfull/exec/execution.hpp>

#include <lexec/execution.hpp>

#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/WaitGroup.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>

namespace ex = lexec;
namespace exec = stackfull::exec;
using namespace stackfull::sched;
using stackfull::sync::WaitGroup;
using std::chrono::milliseconds;

// --- allocation counting -----------------------------------------------------------
// Counts operator new while a test opens the window; nothing else runs then.

namespace {
std::atomic<bool> countingAllocations{false};
std::atomic<long> allocations{0};

void *allocate(std::size_t const size) {
    if (countingAllocations.load(std::memory_order_relaxed)) {
        allocations.fetch_add(1, std::memory_order_relaxed);
    }
    void *const memory = std::malloc(size != 0 ? size : 1);
    if (memory == nullptr) {
        std::abort();
    }
    return memory;
}
} // namespace

void *operator new(std::size_t const size) {
    return allocate(size);
}
void *operator new[](std::size_t const size) {
    return allocate(size);
}
void *operator new(std::size_t const size, std::nothrow_t const &) noexcept {
    return allocate(size);
}
void *operator new[](std::size_t const size, std::nothrow_t const &) noexcept {
    return allocate(size);
}
void operator delete(void *const memory, std::nothrow_t const &) noexcept {
    std::free(memory);
}
void operator delete[](void *const memory, std::nothrow_t const &) noexcept {
    std::free(memory);
}
void operator delete(void *const memory) noexcept {
    std::free(memory);
}
void operator delete[](void *const memory) noexcept {
    std::free(memory);
}
void operator delete(void *const memory, std::size_t) noexcept {
    std::free(memory);
}
void operator delete[](void *const memory, std::size_t) noexcept {
    std::free(memory);
}

namespace {

std::unique_ptr<Scheduler> schedulerWith(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    return makeScheduler(options);
}

template <class F>
void runInTask(Scheduler &scheduler, F body) {
    WaitGroup done;
    done.add(1);
    ASSERT_TRUE(scheduler.spawn([&] {
        body();
        done.done();
    }));
    done.wait();
}

bool waitIdle(Scheduler &scheduler) {
    auto const giveUp = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (scheduler.liveTasks() != 0) {
        if (std::chrono::steady_clock::now() > giveUp) {
            return false;
        }
        std::this_thread::sleep_for(milliseconds{1});
    }
    return true;
}

// Completes with set_stopped once its receiver's token asks; never otherwise.
struct until_stopped_sender {
    using sender_concept = ex::sender_t;
    using completion_signatures = ex::completion_signatures<ex::set_value_t(), ex::set_stopped_t()>;

    template <class Rcvr>
    struct operation {
        using operation_state_concept = ex::operation_state_t;
        using token_type = ex::stop_token_of_t<ex::env_of_t<Rcvr>>;

        struct on_stop {
            void operator()() noexcept { ex::set_stopped(static_cast<Rcvr &&>(self->rcvr)); }
            operation *self;
        };

        explicit operation(Rcvr &&rcvr_) : rcvr(static_cast<Rcvr &&>(rcvr_)) {}
        operation(operation &&) = delete;

        void start() & noexcept { callback.emplace(ex::get_stop_token(ex::get_env(rcvr)), on_stop{this}); }

        Rcvr rcvr;
        std::optional<ex::stop_callback_for_t<token_type, on_stop>> callback;
    };

    template <class Rcvr>
    operation<Rcvr> connect(Rcvr rcvr) const {
        return operation<Rcvr>{static_cast<Rcvr &&>(rcvr)};
    }
};

// Declares a value but completes at once with `Completion`.
template <class Completion>
struct completes_with_sender {
    using sender_concept = ex::sender_t;
    using completion_signatures =
        ex::completion_signatures<ex::set_value_t(int), ex::set_error_t(std::error_code), ex::set_stopped_t()>;

    template <class Rcvr>
    struct operation {
        using operation_state_concept = ex::operation_state_t;
        void start() & noexcept { Completion{}(static_cast<Rcvr &&>(rcvr)); }
        Rcvr rcvr;
    };

    template <class Rcvr>
    operation<Rcvr> connect(Rcvr rcvr) const {
        return operation<Rcvr>{static_cast<Rcvr &&>(rcvr)};
    }
};

struct send_stopped {
    template <class Rcvr>
    void operator()(Rcvr &&rcvr) const noexcept {
        ex::set_stopped(static_cast<Rcvr &&>(rcvr));
    }
};

struct send_io_error {
    template <class Rcvr>
    void operator()(Rcvr &&rcvr) const noexcept {
        ex::set_error(static_cast<Rcvr &&>(rcvr), std::make_error_code(std::errc::io_error));
    }
};

struct recording_receiver {
    using receiver_concept = ex::receiver_t;

    template <class... Vs>
    void set_value(Vs &&...) && noexcept {
        values->fetch_add(1);
    }
    template <class E>
    void set_error(E &&) && noexcept {
        errors->fetch_add(1);
    }
    void set_stopped() && noexcept { stops->fetch_add(1); }
    ex::env<> get_env() const noexcept { return {}; }

    std::atomic<int> *values;
    std::atomic<int> *errors;
    std::atomic<int> *stops;
};

} // namespace

// --- exec::scheduler -----------------------------------------------------------------

TEST(ExecScheduler, IsALexecScheduler) {
    auto scheduler = schedulerWith(1);
    exec::scheduler const sf{*scheduler};
    static_assert(ex::is_scheduler_v<exec::scheduler>);
    EXPECT_TRUE(sf == exec::scheduler{*scheduler});
    EXPECT_EQ(ex::get_forward_progress_guarantee(sf), ex::forward_progress_guarantee::parallel);
    EXPECT_TRUE(ex::get_completion_scheduler<ex::set_value_t>(ex::get_env(ex::schedule(sf))) == sf);
}

TEST(ExecScheduler, WhatFollowsScheduleRunsInATaskOfTheScheduler) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    auto const result = ex::sync_wait(ex::schedule(sf) | ex::then([&] {
                                          return this_task::isTask() and &this_task::scheduler() == scheduler.get();
                                      }));
    ASSERT_TRUE(result);
    EXPECT_TRUE(std::get<0>(*result));
}

TEST(ExecScheduler, StartsOnAndContinuesOn) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    auto const started = ex::sync_wait(ex::starts_on(sf, ex::just(1) | ex::then([](int const v) {
                                                             return this_task::isTask() ? v : -1;
                                                         })));
    ASSERT_TRUE(started);
    EXPECT_EQ(std::get<0>(*started), 1);
    auto const continued =
        ex::sync_wait(ex::just(2) | ex::continues_on(sf) | ex::then([](int const v) { return this_task::isTask() ? v : -1; }));
    ASSERT_TRUE(continued);
    EXPECT_EQ(std::get<0>(*continued), 2);
}

TEST(ExecScheduler, AStoppedSchedulerCompletesScheduleWithStopped) {
    auto scheduler = schedulerWith(1);
    exec::scheduler const sf{*scheduler};
    scheduler->stop();
    EXPECT_FALSE(ex::sync_wait(ex::schedule(sf)));
}

// --- exec::wait ----------------------------------------------------------------------

TEST(ExecWait, ReturnsTheValuesOfASenderThatCompletesInline) {
    auto scheduler = schedulerWith(1);
    std::optional<std::tuple<int, double>> result;
    runInTask(*scheduler, [&] { result = exec::wait(ex::just(1, 2.5)); });
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, std::make_tuple(1, 2.5));
}

TEST(ExecWait, ParksUntilAPoolCompletesAndContinuesOnItsOwnScheduler) {
    auto scheduler = schedulerWith(2);
    ex::static_thread_pool pool{1};
    std::thread::id poolThread;
    std::thread::id after;
    bool ownSchedulerAfter = false;
    runInTask(*scheduler, [&] {
        auto const result =
            exec::wait(ex::schedule(pool.get_scheduler()) | ex::then([] { return std::this_thread::get_id(); }));
        poolThread = std::get<0>(*result);
        after = std::this_thread::get_id();
        ownSchedulerAfter = this_task::isTask() and &this_task::scheduler() == scheduler.get();
    });
    EXPECT_NE(poolThread, after);
    EXPECT_TRUE(ownSchedulerAfter);
}

// The counter is global, so nothing else may run while it counts: one
// worker, and a first task that leaves every thread's lazily created state
// (thread state, stack caches, parkers) behind it.
TEST(ExecWait, AllocatesNothing) {
    auto scheduler = schedulerWith(1);
    ex::static_thread_pool pool{1};
    auto const hop = [&] { return exec::wait(ex::schedule(pool.get_scheduler()) | ex::then([] { return 7; })); };
    runInTask(*scheduler, [&] { hop(); });
    long counterWorks = -1;
    long counted = -1;
    runInTask(*scheduler, [&] {
        allocations.store(0);
        countingAllocations.store(true);
        ::operator delete(::operator new(16));
        countingAllocations.store(false);
        counterWorks = allocations.load();

        allocations.store(0);
        countingAllocations.store(true);
        for (int i = 0; i < 100; ++i) {
            hop();
        }
        countingAllocations.store(false);
        counted = allocations.load();
    });
    EXPECT_EQ(counterWorks, 1);
    EXPECT_EQ(counted, 0);
}

TEST(ExecWait, ReportsStoppedAsAnEmptyOptional) {
    auto scheduler = schedulerWith(1);
    bool stopped = false;
    runInTask(*scheduler, [&] { stopped = not exec::wait(completes_with_sender<send_stopped>{}).has_value(); });
    EXPECT_TRUE(stopped);
}

#if LEXEC_HAS_EXCEPTIONS
TEST(ExecWait, RethrowsErrors) {
    auto scheduler = schedulerWith(1);
    std::error_code code;
    bool runtimeError = false;
    runInTask(*scheduler, [&] {
        try {
            exec::wait(completes_with_sender<send_io_error>{});
        } catch (std::system_error const &error) {
            code = error.code();
        }
        try {
            exec::wait(ex::just(1) | ex::then([](int) -> int { throw std::runtime_error("boom"); }));
        } catch (std::runtime_error const &) {
            runtimeError = true;
        }
    });
    EXPECT_EQ(code, std::errc::io_error);
    EXPECT_TRUE(runtimeError);
}

TEST(ExecWait, AwaitUnpacksValuesAndThrowsOnStopped) {
    auto scheduler = schedulerWith(1);
    int one = 0;
    std::tuple<int, char> two;
    bool stoppedThrown = false;
    runInTask(*scheduler, [&] {
        one = exec::await(ex::just(5));
        exec::await(ex::just());
        two = exec::await(ex::just(1, 'x'));
        try {
            exec::await(completes_with_sender<send_stopped>{});
        } catch (exec::stopped_error const &) {
            stoppedThrown = true;
        }
    });
    EXPECT_EQ(one, 5);
    EXPECT_EQ(two, std::make_tuple(1, 'x'));
    EXPECT_TRUE(stoppedThrown);
}
#endif

TEST(ExecWait, TheSenderSeesTheTaskSchedulerAndAStoppableToken) {
    auto scheduler = schedulerWith(1);
    bool sameScheduler = false;
    bool stoppable = false;
    runInTask(*scheduler, [&] {
        auto const sch = exec::wait(ex::read_env(ex::get_scheduler));
        sameScheduler = std::get<0>(*sch) == exec::scheduler{*scheduler};
        auto const token = exec::wait(ex::read_env(ex::get_stop_token));
        stoppable = std::get<0>(*token).stop_possible();
    });
    EXPECT_TRUE(sameScheduler);
    EXPECT_TRUE(stoppable);
}

TEST(ExecWait, WaitsForSpawnedTasksInTheSameScheduler) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    std::optional<std::tuple<int, int>> both;
    runInTask(*scheduler, [&] {
        both = exec::wait(ex::when_all(exec::spawn(sf, [] { return 1; }), exec::spawn(sf, [] { return 2; })));
    });
    ASSERT_TRUE(both);
    EXPECT_EQ(*both, std::make_tuple(1, 2));
}

// stop() does not unwind a task inside wait: it asks the sender to stop,
// and the task leaves on its own once the sender has completed.
TEST(ExecWait, StopAsksTheSenderToStopInsteadOfUnwindingTheTask) {
    auto scheduler = schedulerWith(2);
    std::atomic<bool> waiting{false};
    std::atomic<bool> sawStopped{false};
    std::atomic<bool> leftNormally{false};
    ASSERT_TRUE(scheduler->spawn([&] {
        waiting.store(true);
        sawStopped.store(not exec::wait(until_stopped_sender{}).has_value());
        leftNormally.store(true);
    }));
    while (not waiting.load()) {
        std::this_thread::sleep_for(milliseconds{1});
    }
    std::this_thread::sleep_for(milliseconds{10});
    scheduler->stop();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_TRUE(sawStopped.load());
    EXPECT_TRUE(leftNormally.load());
}

// --- exec::spawn ---------------------------------------------------------------------

TEST(ExecSpawn, CompletesWithTheValueOfTheTask) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    auto const result = ex::sync_wait(exec::spawn(sf, [&] { return this_task::isTask() ? 42 : -1; }));
    ASSERT_TRUE(result);
    EXPECT_EQ(std::get<0>(*result), 42);
    int ran = 0;
    EXPECT_TRUE(ex::sync_wait(exec::spawn(sf, [&] { ++ran; })));
    EXPECT_EQ(ran, 1);
}

#if LEXEC_HAS_EXCEPTIONS
TEST(ExecSpawn, ExceptionsBecomeErrorsAndStoppedErrorBecomesStopped) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    EXPECT_THROW(ex::sync_wait(exec::spawn(sf, []() -> int { throw std::runtime_error("boom"); })), std::runtime_error);
    EXPECT_FALSE(ex::sync_wait(exec::spawn(sf, []() -> int { throw exec::stopped_error{}; })));
    // An awaited sender's stop ends the task with stopped too.
    EXPECT_FALSE(ex::sync_wait(exec::spawn(sf, [] { return exec::await(completes_with_sender<send_stopped>{}); })));
}
#endif

TEST(ExecSpawn, AStoppedSchedulerCompletesWithStopped) {
    auto scheduler = schedulerWith(1);
    exec::scheduler const sf{*scheduler};
    scheduler->stop();
    EXPECT_FALSE(ex::sync_wait(exec::spawn(sf, [] { return 1; })));
}

// The receiver's stop request reaches the senders the task waits for.
TEST(ExecSpawn, TheReceiversStopRequestsReachWaitsInTheTask) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    std::atomic<bool> taskSawStopped{false};
    auto const winner = ex::sync_wait(ex::when_any(exec::spawn(sf,
                                                               [&] {
                                                                   taskSawStopped.store(
                                                                       not exec::wait(until_stopped_sender{}).has_value());
                                                                   return 1;
                                                               }),
                                                   ex::just(2)));
    ASSERT_TRUE(winner);
    EXPECT_EQ(std::get<0>(*winner), 2);
    EXPECT_TRUE(taskSawStopped.load());
}

// Scheduler::stop() ends a task parked outside exec::wait early (unwound,
// or dropped without exceptions): its receiver still completes, once, with
// stopped.
TEST(ExecSpawn, ATaskStopEndsEarlyCompletesItsReceiverWithStoppedOnce) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    std::atomic<int> values{0};
    std::atomic<int> errors{0};
    std::atomic<int> stops{0};
    std::atomic<bool> parked{false};
    auto op = ex::connect(exec::spawn(sf,
                                      [&] {
                                          parked.store(true);
                                          for (;;) {
                                              this_task::park(); // leaves only when stop() ends it
                                          }
                                      }),
                          recording_receiver{&values, &errors, &stops});
    ex::start(op);
    while (not parked.load()) {
        std::this_thread::sleep_for(milliseconds{1});
    }
    scheduler->stop();
    ASSERT_TRUE(waitIdle(*scheduler));
    EXPECT_EQ(stops.load(), 1);
    EXPECT_EQ(values.load(), 0);
    EXPECT_EQ(errors.load(), 0);
}

TEST(ExecSpawn, RunsUnderACountingScope) {
    auto scheduler = schedulerWith(2);
    exec::scheduler const sf{*scheduler};
    ex::counting_scope scope;
    std::atomic<int> ran{0};
    for (int i = 0; i < 10; ++i) {
        ex::spawn(exec::spawn(sf, [&] { ran.fetch_add(1); }) | ex::upon_error([](auto &&) noexcept {}),
                  scope.get_token());
    }
    ex::sync_wait(scope.join());
    EXPECT_EQ(ran.load(), 10);
}
