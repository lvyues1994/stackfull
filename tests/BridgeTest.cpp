// Bridging callback-style SDKs into tasks: Completion, Stream, Mailbox,
// select, and the runtime entry points (go / async / blockOn).

#include <stackfull/io/TcpStream.h>
#include <stackfull/runtime/Runtime.h>
#include <stackfull/sched/Scheduler.h>
#include <stackfull/sched/Sleep.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sync/Completion.h>
#include <stackfull/sync/Mailbox.h>
#include <stackfull/sync/Select.h>
#include <stackfull/sync/Stream.h>
#include <stackfull/sync/WaitGroup.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#if STACKFULL_HAS_EXCEPTIONS
#include <stdexcept>
#endif

using namespace stackfull;
using namespace stackfull::sched;
using namespace stackfull::sync;
using std::chrono::milliseconds;

// ---------------------------------------------------------------------------
// Three fake SDKs. None of them knows anything about the scheduler; all of
// them call back from their own threads.
// ---------------------------------------------------------------------------
namespace {

// 1. std::function callback with an SDK-specific signature.
struct AsyncSdk {
    using OnRead = std::function<void(int result, std::error_code error)>;
    void readAsync(int const handle, OnRead callback, milliseconds const delay = milliseconds{5}) {
        std::thread([handle, callback, delay] {
            std::this_thread::sleep_for(delay);
            callback(handle * 10, std::error_code{});
        }).detach();
    }
};

// 2. C API: function pointer + void *user.
extern "C" typedef void (*c_read_cb)(void *user, int rc, char const *data, std::size_t length);
struct CSdk {
    static void read(int const handle, c_read_cb const callback, void *const user) {
        std::thread([handle, callback, user] {
            std::this_thread::sleep_for(milliseconds{5});
            std::string const payload = "payload-" + std::to_string(handle);
            callback(user, 0, payload.data(), payload.size());
        }).detach();
    }
};

// 3. Listener interface: the SDK holds a raw pointer and calls virtuals from
//    its capture thread, 3 ms apart.
struct Frame {
    int sequence = 0;
};
struct CameraListener {
    virtual ~CameraListener() = default;
    virtual void onFrame(Frame const &frame) = 0;
    virtual void onError(int code) = 0;
    virtual void onStopped() = 0;
};
struct CameraSdk {
    void start(CameraListener *const listener, int const frames) {
        thread = std::thread([listener, frames] {
            for (int i = 0; i < frames; ++i) {
                std::this_thread::sleep_for(milliseconds{3});
                listener->onFrame(Frame{i});
            }
            listener->onError(42);
            listener->onStopped();
        });
    }
    void join() {
        if (thread.joinable()) {
            thread.join();
        }
    }
    std::thread thread;
};

std::unique_ptr<Scheduler> schedulerWith(std::size_t const workers) {
    SchedulerOptions options;
    options.workers = workers;
    return makeScheduler(options);
}

} // namespace

// ---------------------------------------------------------------------------
// Completion
// ---------------------------------------------------------------------------

TEST(Completion, IsTheCallbackAndTheAwaitable) {
    auto scheduler = schedulerWith(2);
    AsyncSdk sdk;
    WaitGroup done;
    done.add(1);
    int result = 0;
    ASSERT_TRUE(scheduler->spawn([&] {
        Completion<std::pair<int, std::error_code>> read;
        sdk.readAsync(7, read);             // the handle *is* the callback
        auto outcome = read.get();          // task parks; SDK thread completes it
        ASSERT_TRUE(outcome);
        result = outcome.value.first;
        done.done();
    }));
    done.wait();
    EXPECT_EQ(result, 70);
}

TEST(Completion, TypeDeducedFromTheSdkSignature) {
    static_assert(std::is_same<CompletionFor<AsyncSdk::OnRead>, Completion<std::tuple<int, std::error_code>>>::value,
                  "two arguments become a tuple");
    static_assert(std::is_same<CompletionFor<void(int)>, Completion<int>>::value, "one argument is the value");
    static_assert(std::is_same<CompletionFor<void()>, Completion<void>>::value, "no argument is void");
    static_assert(std::is_same<CompletionFor<void (*)(std::string const &)>, Completion<std::string>>::value,
                  "function pointers, decayed");

    auto scheduler = schedulerWith(1);
    AsyncSdk sdk;
    WaitGroup done;
    done.add(1);
    int result = 0;
    ASSERT_TRUE(scheduler->spawn([&] {
        auto read = completionFor<AsyncSdk::OnRead>();
        sdk.readAsync(3, read);
        result = std::get<0>(read.get().value);
        done.done();
    }));
    done.wait();
    EXPECT_EQ(result, 30);
}

TEST(Completion, SetBeforeGetReturnsImmediately) {
    Completion<int> completion;
    completion.set(5);
    EXPECT_TRUE(completion.isReady());
    EXPECT_EQ(completion.get().value, 5); // from a plain thread, no parking
}

TEST(Completion, FirstSetWinsAndFailIsReported) {
    Completion<int> completion;
    completion.fail(std::make_error_code(std::errc::io_error));
    completion.set(1);
    auto const outcome = completion.get();
    EXPECT_FALSE(outcome);
    EXPECT_EQ(outcome.error, std::errc::io_error);
}

TEST(Completion, GetForTimesOutAndALateSetIsHarmless) {
    auto scheduler = schedulerWith(2);
    AsyncSdk sdk;
    WaitGroup done;
    done.add(1);
    std::error_code seen;
    Completion<std::pair<int, std::error_code>> read;
    ASSERT_TRUE(scheduler->spawn([&] {
        sdk.readAsync(1, read, milliseconds{60}); // slower than our patience
        seen = read.getFor(milliseconds{10}).error;
        done.done();
    }));
    done.wait();
    EXPECT_EQ(seen, std::errc::timed_out);
    std::this_thread::sleep_for(milliseconds{80}); // the SDK still completes into shared state
    EXPECT_TRUE(read.isReady());
}

TEST(Completion, ThroughACVoidPointer) {
    auto scheduler = schedulerWith(1);
    WaitGroup done;
    done.add(1);
    std::string received;
    struct Trampoline {
        static void onRead(void *const user, int, char const *const data, std::size_t const length) {
            Completion<std::string>::fromRaw(user).set(std::string(data, length));
        }
    };
    ASSERT_TRUE(scheduler->spawn([&] {
        Completion<std::string> read;
        CSdk::read(9, &Trampoline::onRead, read.toRaw());
        received = read.get().value;
        done.done();
    }));
    done.wait();
    EXPECT_EQ(received, "payload-9");
}

// The awaiting task is torn down by stop() — unwound where exceptions are
// available, released outright where they are not — and only then does the
// SDK call back. The scheduler itself is still alive, as WakeToken requires.
TEST(Completion, CallbackAfterTheAwaitingTaskWasTornDownIsSafe) {
    Completion<std::pair<int, std::error_code>> read;
    AsyncSdk sdk;
    auto scheduler = schedulerWith(2);
    std::atomic<bool> waiting{false};
    ASSERT_TRUE(scheduler->spawn([&] {
        waiting.store(true);
        read.get(); // never completes before the scheduler stops
    }));
    while (not waiting.load()) {
    }
    std::this_thread::sleep_for(milliseconds{5});
    scheduler->stop();
    while (scheduler->liveTasks() != 0) {
        std::this_thread::sleep_for(milliseconds{1});
    }
    sdk.readAsync(4, read); // late callback into state nobody awaits
    std::this_thread::sleep_for(milliseconds{30});
    EXPECT_TRUE(read.isReady());
    EXPECT_EQ(read.get().value.first, 40);
}

TEST(Completion, VoidSignalIgnoresCallbackArguments) {
    Completion<void> stopped;
    std::function<void(int, std::string)> const sdkCallback = stopped; // any signature fits
    sdkCallback(1, "ignored");
    EXPECT_TRUE(stopped.isReady());
    EXPECT_TRUE(stopped.get());
}

// ---------------------------------------------------------------------------
// Stream / Latest
// ---------------------------------------------------------------------------

TEST(Stream, DeliversInOrderAndEndsOnClose) {
    auto scheduler = schedulerWith(2);
    auto frames = std::make_shared<Stream<Frame>>(64);
    std::thread producer([frames] {
        for (int i = 0; i < 50; ++i) {
            frames->push(Frame{i});
        }
        frames->close();
    });
    WaitGroup done;
    done.add(1);
    std::vector<int> seen;
    ASSERT_TRUE(scheduler->spawn([&] {
        Frame frame;
        while (frames->next(frame)) {
            seen.push_back(frame.sequence);
        }
        done.done();
    }));
    done.wait();
    producer.join();
    ASSERT_EQ(seen.size(), 50u);
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(seen[static_cast<std::size_t>(i)], i);
    }
    EXPECT_EQ(frames->dropped(), 0u);
}

TEST(Stream, DropOldestKeepsTheFreshestWhenTheConsumerIsSlow) {
    Stream<int> stream(2, Overflow::DropOldest);
    for (int i = 0; i < 10; ++i) {
        stream.push(i);
    }
    int value = -1;
    ASSERT_TRUE(stream.tryNext(value));
    EXPECT_EQ(value, 8);
    ASSERT_TRUE(stream.tryNext(value));
    EXPECT_EQ(value, 9);
    EXPECT_FALSE(stream.tryNext(value));
    EXPECT_EQ(stream.dropped(), 8u);
}

TEST(Stream, DropNewestKeepsWhatWasAccepted) {
    Stream<int> stream(2, Overflow::DropNewest);
    for (int i = 0; i < 10; ++i) {
        stream.push(i);
    }
    int value = -1;
    ASSERT_TRUE(stream.tryNext(value));
    EXPECT_EQ(value, 0);
    ASSERT_TRUE(stream.tryNext(value));
    EXPECT_EQ(value, 1);
    EXPECT_EQ(stream.dropped(), 8u);
}

TEST(Stream, LatestHoldsOneItem) {
    Latest<Frame> latest;
    latest.push(Frame{1});
    latest.push(Frame{2});
    latest.push(Frame{3});
    Frame frame;
    ASSERT_TRUE(latest.tryNext(frame));
    EXPECT_EQ(frame.sequence, 3);
    EXPECT_FALSE(latest.tryNext(frame));
}

TEST(Stream, NextForTimesOutWithoutAProducer) {
    auto scheduler = schedulerWith(1);
    Stream<int> stream(4);
    WaitGroup done;
    done.add(1);
    bool got = true;
    milliseconds waited{0};
    ASSERT_TRUE(scheduler->spawn([&] {
        int value = 0;
        auto const start = std::chrono::steady_clock::now();
        got = stream.nextFor(milliseconds{15}, value);
        waited = std::chrono::duration_cast<milliseconds>(std::chrono::steady_clock::now() - start);
        done.done();
    }));
    done.wait();
    EXPECT_FALSE(got);
    EXPECT_FALSE(stream.isClosed());
    EXPECT_GE(waited.count(), 15);
}

TEST(Stream, CameraStyleThirtyMillisecondFrames) {
    auto scheduler = schedulerWith(2);
    auto frames = std::make_shared<Latest<Frame>>();
    std::atomic<bool> stop{false};
    std::thread camera([frames, &stop] {
        for (int i = 0; not stop.load(); ++i) {
            std::this_thread::sleep_for(milliseconds{3});
            frames->push(Frame{i});
        }
        frames->close();
    });
    WaitGroup done;
    done.add(1);
    int processed = 0;
    int lastSequence = -1;
    bool monotonic = true;
    ASSERT_TRUE(scheduler->spawn([&] {
        Frame frame;
        while (frames->nextFor(milliseconds{100}, frame)) {
            monotonic = monotonic and frame.sequence > lastSequence;
            lastSequence = frame.sequence;
            if (++processed == 20) {
                stop.store(true); // enough
            }
            if (frames->isClosed()) {
                break;
            }
        }
        done.done();
    }));
    done.wait();
    camera.join();
    EXPECT_GE(processed, 20);
    EXPECT_TRUE(monotonic);
}

// ---------------------------------------------------------------------------
// Mailbox: the listener-interface adapter
// ---------------------------------------------------------------------------

namespace {

struct CameraBridge final : CameraListener {
    Mailbox inbox;
    std::shared_ptr<CameraBridge> self; // alive until the SDK says stopped
    std::vector<std::string> log;       // touched only from the owning task

    void onFrame(Frame const &frame) override {
        inbox.post([this, frame] { log.push_back("frame " + std::to_string(frame.sequence)); });
    }
    void onError(int const code) override {
        inbox.post([this, code] {
            this_task::sleepFor(milliseconds{2}); // task context: parking is fine here
            log.push_back("error " + std::to_string(code));
        });
    }
    void onStopped() override {
        inbox.post([this] {
            log.push_back("stopped");
            inbox.close();
        });
        self.reset();
    }
};

} // namespace

TEST(Mailbox, ListenerCallsRunInOrderInTaskContext) {
    auto scheduler = schedulerWith(2);
    CameraSdk camera;
    auto bridge = std::make_shared<CameraBridge>();
    bridge->self = bridge;
    WaitGroup done;
    done.add(1);
    std::vector<std::string> log;
    ASSERT_TRUE(scheduler->spawn([&, bridge] {
        camera.start(bridge.get(), 5);
        bridge->inbox.run(); // until onStopped closes it
        log = bridge->log;
        done.done();
    }));
    done.wait();
    camera.join();
    std::vector<std::string> const expected{"frame 0", "frame 1", "frame 2", "frame 3", "frame 4", "error 42",
                                            "stopped"};
    EXPECT_EQ(log, expected);
}

TEST(Mailbox, PostAfterCloseIsDropped) {
    Mailbox inbox;
    int ran = 0;
    inbox.post([&] { ++ran; });
    inbox.close();
    inbox.post([&] { ++ran; });
    inbox.run(); // drains the one accepted before close
    EXPECT_EQ(ran, 1);
}

TEST(Mailbox, RunOneForTimesOut) {
    auto scheduler = schedulerWith(1);
    Mailbox inbox;
    WaitGroup done;
    done.add(1);
    bool ran = true;
    ASSERT_TRUE(scheduler->spawn([&] {
        ran = inbox.runOneFor(milliseconds{10});
        done.done();
    }));
    done.wait();
    EXPECT_FALSE(ran);
    EXPECT_FALSE(inbox.isClosed());
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

TEST(Select, ReturnsWhicheverSourceFiresFirst) {
    auto scheduler = schedulerWith(2);
    Completion<int> control;
    Stream<int> data(4);
    WaitGroup done;
    done.add(1);
    std::vector<std::size_t> order;
    ASSERT_TRUE(scheduler->spawn([&] {
        order.push_back(select(control, data)); // data arrives first
        int value = 0;
        ASSERT_TRUE(data.tryNext(value));
        order.push_back(select(control, data)); // then control
        done.done();
    }));
    std::this_thread::sleep_for(milliseconds{5});
    data.push(1);
    std::this_thread::sleep_for(milliseconds{5});
    control.set(2);
    done.wait();
    std::vector<std::size_t> const expected{1, 0};
    EXPECT_EQ(order, expected);
}

TEST(Select, TimesOut) {
    auto scheduler = schedulerWith(1);
    Completion<int> never;
    Stream<int> silent(1);
    WaitGroup done;
    done.add(1);
    std::size_t result = 0;
    ASSERT_TRUE(scheduler->spawn([&] {
        result = selectFor(milliseconds{10}, never, silent);
        done.done();
    }));
    done.wait();
    EXPECT_EQ(result, kSelectTimedOut);
}

TEST(Select, WorksFromAPlainThread) {
    Completion<int> a;
    Completion<int> b;
    std::thread producer([&] {
        std::this_thread::sleep_for(milliseconds{5});
        b.set(1);
    });
    EXPECT_EQ(select(a, b), 1u);
    producer.join();
}

// ---------------------------------------------------------------------------
// Runtime entry points
// ---------------------------------------------------------------------------

TEST(Runtime, GoAsyncBlockOn) {
    int const answer = blockOn([] {
        auto const doubled = async([] { return 21 * 2; });
        auto const signal = async([] { this_task::sleepFor(milliseconds{2}); });
        EXPECT_TRUE(signal.get());
        return doubled.get().value;
    });
    EXPECT_EQ(answer, 42);
    EXPECT_EQ(&defaultScheduler(), &defaultScheduler());
    EXPECT_TRUE(go([] {}));
}

TEST(Runtime, DefaultSchedulerDoesIoThroughDefaultPoller) {
    std::string const reply = blockOn([] {
        io::TcpListenerResult bound = io::TcpListener::bind(defaultPoller(), io::SocketAddress::loopback(0));
        if (not bound) {
            return std::string("bind: ") + bound.error.message();
        }
        io::TcpListener &listener = *bound.listener;
        auto const served = async([&listener] {
            io::TcpStreamResult peer = listener.accept();
            char buffer[4];
            if (peer and peer.stream->readExactly(buffer, 4).bytes == 4) {
                peer.stream->writeAll("pong", 4);
            }
        });
        io::TcpStreamResult client =
            io::TcpStream::connect(defaultPoller(), io::SocketAddress::loopback(listener.port()));
        if (not client) {
            return std::string("connect: ") + client.error.message();
        }
        client.stream->writeAll("ping", 4);
        char buffer[4];
        io::IoResult const got = client.stream->readExactly(buffer, 4);
        served.get();
        return std::string(buffer, got.bytes);
    });
    EXPECT_EQ(reply, "pong");
}

TEST(Runtime, BlockingKeepsTheWorkerFree) {
    SchedulerOptions options;
    options.workers = 1; // a blocked worker would stall everything
    auto scheduler = makeScheduler(options);
    std::atomic<int> ticks{0};
    std::atomic<bool> stop{false};
    int result = 0;
    int ticksWhileBlocked = 0;
    WaitGroup done;
    done.add(2);
    ASSERT_TRUE(scheduler->spawn([&] {
        while (not stop.load()) {
            this_task::sleepFor(milliseconds{1});
            ticks.fetch_add(1);
        }
        done.done();
    }));
    ASSERT_TRUE(scheduler->spawn([&] {
        int const before = ticks.load();
        result = blocking([] {
            std::this_thread::sleep_for(milliseconds{60}); // a truly blocking call
            return 7;
        });
        ticksWhileBlocked = ticks.load() - before;
        stop.store(true);
        done.done();
    }));
    done.wait();
    EXPECT_EQ(result, 7);
    EXPECT_GE(ticksWhileBlocked, 10);
}

TEST(Runtime, BlockingFromAPlainThreadRunsInline) {
    std::thread::id ranOn;
    int const value = blocking([&] {
        ranOn = std::this_thread::get_id();
        return 3;
    });
    EXPECT_EQ(value, 3);
    EXPECT_EQ(ranOn, std::this_thread::get_id());
}

#if STACKFULL_HAS_EXCEPTIONS
TEST(Runtime, BlockingRethrowsInTheTask) {
    bool caught = blockOn([] {
        try {
            blocking([]() -> int { throw std::runtime_error("disk on fire"); });
        } catch (std::runtime_error const &) {
            return true;
        }
        return false;
    });
    EXPECT_TRUE(caught);
}
#endif

TEST(Runtime, BlockOnVoid) {
    bool ran = false;
    blockOn([&] { ran = true; });
    EXPECT_TRUE(ran);
}

#if STACKFULL_HAS_EXCEPTIONS
TEST(Runtime, AsyncCapturesExceptionsAndBlockOnRethrows) {
    auto const failed = async([]() -> int { throw std::runtime_error("boom"); });
    auto const outcome = failed.get();
    EXPECT_FALSE(outcome);
    EXPECT_TRUE(static_cast<bool>(outcome.exception));
    EXPECT_THROW(blockOn([]() -> int { throw std::runtime_error("boom"); }), std::runtime_error);
}
#endif
