#include <stackfull/coro/Coroutine.h>
#include <stackfull/coro/ForcedUnwind.h>
#include <stackfull/stack/MmapStackAllocator.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#include <stdexcept>
#endif

using namespace stackfull::coro;
using stackfull::stack::StackAllocation;
using stackfull::stack::StackAllocator;
using stackfull::stack::StackView;

namespace {

bool isFrameAligned16() noexcept {
    return reinterpret_cast<std::uintptr_t>(__builtin_frame_address(0)) % 16 == 0;
}

// GCC's -fipa-stack-alignment (on even at -O0) may call a *known* leaf with a
// misaligned stack. Calling through an opaque pointer forces the ABI, so the
// probe measures the alignment our trampoline established, not a compiler
// shortcut.
using AlignmentProbe = bool (*)();
AlignmentProbe volatile alignmentProbe = &isFrameAligned16;

// pthread_self() is declared `const` by glibc, so the optimizer may merge two
// std::this_thread::get_id() calls across a yield — precisely the hazard
// documented for code that migrates between threads. The barrier keeps clang
// from inferring the wrapper itself is side-effect free and merging the calls.
__attribute__((noinline)) std::thread::id currentThreadIdNow() {
    asm volatile("" ::: "memory");
    return std::this_thread::get_id();
}

// Records construction / destruction into a shared log.
struct Tracer {
    explicit Tracer(std::vector<std::string> &log_, std::string name_) : log(log_), name(std::move(name_)) {
        log.push_back("ctor " + name);
    }
    ~Tracer() { log.push_back("dtor " + name); }
    Tracer(Tracer const &) = delete;
    Tracer &operator=(Tracer const &) = delete;

    std::vector<std::string> &log;
    std::string name;
};

Coroutine mustMake(std::function<void()> body, CoroutineOptions const &options = CoroutineOptions{}) {
    CoroutineCreation created = makeCoroutine(std::move(body), options);
    EXPECT_TRUE(created) << created.error.message();
    return std::move(created.coroutine);
}

struct FailingAllocator final : StackAllocator {
    StackAllocation allocate(std::size_t) noexcept override {
        return StackAllocation{StackView{}, std::make_error_code(std::errc::not_enough_memory)};
    }
    void deallocate(StackView const &) noexcept override {}
};

} // namespace

// ---------------------------------------------------------------------------
// Basic control flow
// ---------------------------------------------------------------------------

TEST(Coroutine, ResumeYieldResumeFinish) {
    std::vector<std::string> log;
    Coroutine coroutine = mustMake([&] {
        log.push_back("start");
        Coroutine::yield();
        log.push_back("middle");
        Coroutine::yield();
        log.push_back("end");
    });

    EXPECT_EQ(coroutine.state(), CoroutineState::Created);
    coroutine.resume();
    EXPECT_EQ(coroutine.state(), CoroutineState::Suspended);
    log.push_back("main1");
    coroutine.resume();
    log.push_back("main2");
    coroutine.resume();
    EXPECT_TRUE(coroutine.isDone());

    std::vector<std::string> const expected{"start", "main1", "middle", "main2", "end"};
    EXPECT_EQ(log, expected);
}

TEST(Coroutine, EmptyHandleIsFalsy) {
    Coroutine const empty;
    EXPECT_FALSE(empty);
}

TEST(Coroutine, IsInsideCoroutine) {
    EXPECT_FALSE(Coroutine::isInsideCoroutine());
    bool inside = false;
    Coroutine coroutine = mustMake([&] { inside = Coroutine::isInsideCoroutine(); });
    coroutine.resume();
    EXPECT_TRUE(inside);
    EXPECT_FALSE(Coroutine::isInsideCoroutine());
}

TEST(Coroutine, BodyRunsOnItsOwnAlignedStack) {
    bool aligned = false;
    std::uintptr_t bodyFrame = 0;
    Coroutine coroutine = mustMake([&] {
        aligned = alignmentProbe();
        bodyFrame = reinterpret_cast<std::uintptr_t>(__builtin_frame_address(0));
    });
    coroutine.resume();
    EXPECT_TRUE(aligned);
    auto const mainFrame = reinterpret_cast<std::uintptr_t>(__builtin_frame_address(0));
    std::uintptr_t const distance = bodyFrame > mainFrame ? bodyFrame - mainFrame : mainFrame - bodyFrame;
    EXPECT_GT(distance, std::uintptr_t{64} * 1024); // clearly a different stack
}

TEST(Coroutine, NestedResumeReturnsToTheInnerResumer) {
    std::vector<std::string> log;
    Coroutine inner = mustMake([&] {
        log.push_back("inner1");
        Coroutine::yield();
        log.push_back("inner2");
    });
    Coroutine outer = mustMake([&] {
        log.push_back("outer1");
        inner.resume(); // inner yields back *here*, not to main
        log.push_back("outer2");
        Coroutine::yield();
        inner.resume();
        log.push_back("outer3");
    });

    outer.resume();
    log.push_back("main");
    outer.resume();
    EXPECT_TRUE(outer.isDone());
    EXPECT_TRUE(inner.isDone());

    std::vector<std::string> const expected{"outer1", "inner1", "outer2", "main", "inner2", "outer3"};
    EXPECT_EQ(log, expected);
}

TEST(Coroutine, MoveTransfersOwnership) {
    int runs = 0;
    Coroutine first = mustMake([&] {
        runs += 1;
        Coroutine::yield();
        runs += 1;
    });
    first.resume();
    Coroutine second = std::move(first);
    EXPECT_FALSE(first);
    EXPECT_TRUE(second);
    second.resume();
    EXPECT_EQ(runs, 2);
    EXPECT_TRUE(second.isDone());

    Coroutine third;
    third = std::move(second);
    EXPECT_FALSE(second);
    EXPECT_TRUE(third.isDone());
}

TEST(Coroutine, MoveOnlyBodyIsSupported) {
    auto payload = std::make_unique<int>(41);
    int seen = 0;
    CoroutineCreation created = makeCoroutine([p = std::move(payload), &seen] { seen = *p + 1; });
    ASSERT_TRUE(created);
    created.coroutine.resume();
    EXPECT_EQ(seen, 42);
}

// ---------------------------------------------------------------------------
// Lifetime
// ---------------------------------------------------------------------------

TEST(Coroutine, BodyIsDestroyedRightAfterItReturns) {
    std::vector<std::string> log;
    auto tracer = std::make_shared<Tracer>(log, "captured");
    Coroutine coroutine = mustMake([tracer, &log] { log.push_back("run"); });
    tracer.reset();
    coroutine.resume();
    std::vector<std::string> const expected{"ctor captured", "run", "dtor captured"};
    EXPECT_EQ(log, expected);
}

TEST(Coroutine, DestroyingACreatedCoroutineDestroysTheBodyWithoutRunningIt) {
    std::vector<std::string> log;
    bool ran = false;
    {
        auto tracer = std::make_shared<Tracer>(log, "captured");
        Coroutine const coroutine = mustMake([tracer, &ran] { ran = true; });
        tracer.reset();
    }
    EXPECT_FALSE(ran);
    std::vector<std::string> const expected{"ctor captured", "dtor captured"};
    EXPECT_EQ(log, expected);
}

TEST(Coroutine, CooperativeStop) {
    int iterations = 0;
    Coroutine coroutine = mustMake([&] {
        while (not Coroutine::stopRequested()) {
            iterations += 1;
            Coroutine::yield();
        }
    });
    coroutine.resume();
    coroutine.resume();
    EXPECT_EQ(iterations, 2);
    coroutine.requestStop();
    coroutine.resume();
    EXPECT_TRUE(coroutine.isDone());
    EXPECT_EQ(iterations, 2);
}

TEST(Coroutine, ManyCoroutinesWithSmallStacks) {
    CoroutineOptions options;
    options.stackSize = 16 * 1024;
    constexpr int kCount = 10000;
    int finished = 0;
    std::vector<Coroutine> coroutines;
    coroutines.reserve(kCount);
    for (int i = 0; i < kCount; ++i) {
        coroutines.push_back(mustMake(
            [&finished] {
                Coroutine::yield();
                finished += 1;
            },
            options));
    }
    for (Coroutine &c : coroutines) {
        c.resume();
    }
    for (Coroutine &c : coroutines) {
        c.resume();
    }
    EXPECT_EQ(finished, kCount);
}

TEST(Coroutine, DeepRecursionFitsTheStack) {
    struct Recurse {
        static int down(int const depth) {
            char pad[512];
            pad[0] = static_cast<char>(depth);
            return depth == 0 ? pad[0] : down(depth - 1) + 1;
        }
    };
    CoroutineOptions options;
    options.stackSize = 256 * 1024;
    int result = -1;
    Coroutine coroutine = mustMake([&] { result = Recurse::down(200); }, options); // ~100 KiB of frames
    coroutine.resume();
    EXPECT_EQ(result, 200);
}

// ---------------------------------------------------------------------------
// Creation failures
// ---------------------------------------------------------------------------

TEST(Coroutine, ReportsAllocatorFailure) {
    FailingAllocator allocator;
    CoroutineOptions options;
    options.allocator = &allocator;
    CoroutineCreation const created = makeCoroutine([] {}, options);
    EXPECT_FALSE(created);
    EXPECT_FALSE(created.coroutine);
    EXPECT_EQ(created.error, std::errc::not_enough_memory);
}

TEST(Coroutine, RejectsAStackTooSmallForTheControlBlock) {
    CoroutineOptions options;
    options.stackSize = 1; // rounds to one page, below kMinUsableStack after carving
    CoroutineCreation const created = makeCoroutine([] {}, options);
    EXPECT_FALSE(created);
    EXPECT_EQ(created.error, std::errc::invalid_argument);
}

// ---------------------------------------------------------------------------
// Contract violations
// ---------------------------------------------------------------------------

TEST(CoroutineDeath, YieldOutsideACoroutineAborts) {
    EXPECT_DEATH_IF_SUPPORTED(Coroutine::yield(), "yield\\(\\) outside a coroutine");
}

TEST(CoroutineDeath, ResumingAFinishedCoroutineAborts) {
    Coroutine coroutine = mustMake([] {});
    coroutine.resume();
    EXPECT_DEATH_IF_SUPPORTED(coroutine.resume(), "Created or Suspended");
}

TEST(CoroutineDeath, StackOverflowHitsTheGuardPage) {
    struct Forever {
        // `limit` is never reached in practice; it only keeps the recursion
        // formally finite so the compiler does not diagnose it.
        static int recurse(int const n, int const limit) {
            volatile char pad[256];
            pad[0] = static_cast<char>(n);
            return n >= limit ? pad[0] : recurse(n + 1, limit) + pad[0];
        }
    };
    CoroutineOptions options;
    options.stackSize = 32 * 1024;
    EXPECT_DEATH_IF_SUPPORTED(
        {
            Coroutine coroutine = mustMake([] { Forever::recurse(0, 1 << 30); }, options);
            coroutine.resume();
        },
        "");
}

// ---------------------------------------------------------------------------
// Threads: a suspended coroutine may continue on another thread
// ---------------------------------------------------------------------------

TEST(Coroutine, ResumesOnAnotherThread) {
    std::thread::id firstThread;
    std::thread::id secondThread;
    Coroutine coroutine = mustMake([&] {
        firstThread = currentThreadIdNow();
        Coroutine::yield();
        secondThread = currentThreadIdNow();
    });

    coroutine.resume();
    std::thread worker([&] { coroutine.resume(); });
    worker.join();

    EXPECT_TRUE(coroutine.isDone());
    EXPECT_EQ(firstThread, currentThreadIdNow());
    EXPECT_NE(secondThread, firstThread);
}

TEST(Coroutine, CreatedOnOneThreadDrivenEntirelyByAnother) {
    int steps = 0;
    Coroutine coroutine = mustMake([&] {
        steps += 1;
        Coroutine::yield();
        steps += 1;
    });
    std::thread([&] {
        coroutine.resume();
        coroutine.resume();
    }).join();
    EXPECT_EQ(steps, 2);
    EXPECT_TRUE(coroutine.isDone());
}

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------
#if STACKFULL_HAS_EXCEPTIONS

TEST(CoroutineExceptions, EscapingExceptionIsRethrownByResume) {
    Coroutine coroutine = mustMake([] {
        Coroutine::yield();
        throw std::runtime_error("boom");
    });
    coroutine.resume();
    EXPECT_THROW(coroutine.resume(), std::runtime_error);
    EXPECT_TRUE(coroutine.isDone());
}

TEST(CoroutineExceptions, DestroyingASuspendedCoroutineUnwindsItsStack) {
    std::vector<std::string> log;
    {
        Coroutine coroutine = mustMake([&] {
            Tracer const outer(log, "outer");
            {
                Tracer const inner(log, "inner");
                Coroutine::yield();
                log.push_back("not reached");
            }
        });
        coroutine.resume();
        log.push_back("destroying");
    }
    std::vector<std::string> const expected{"ctor outer", "ctor inner", "destroying", "dtor inner", "dtor outer"};
    EXPECT_EQ(log, expected);
}

TEST(CoroutineExceptions, ForcedUnwindIsNotAStdException) {
    bool sawStdException = false;
    bool sawForcedUnwind = false;
    {
        Coroutine coroutine = mustMake([&] {
            try {
                Coroutine::yield();
            } catch (std::exception const &) {
                sawStdException = true;
            } catch (ForcedUnwind const &) {
                sawForcedUnwind = true;
                throw;
            }
        });
        coroutine.resume();
    }
    EXPECT_FALSE(sawStdException);
    EXPECT_TRUE(sawForcedUnwind);
}

TEST(CoroutineExceptions, MoveAssignmentUnwindsThePreviousCoroutine) {
    std::vector<std::string> log;
    Coroutine holder = mustMake([&] {
        Tracer const t(log, "first");
        Coroutine::yield();
    });
    holder.resume();
    holder = mustMake([] {});
    std::vector<std::string> const expected{"ctor first", "dtor first"};
    EXPECT_EQ(log, expected);
}

namespace {

int rethrowCurrentAsInt() {
    try {
        std::rethrow_exception(std::current_exception());
    } catch (int const value) {
        return value;
    } catch (...) {
        return -1;
    }
    return -2;
}

} // namespace

// Two coroutines interleave their catch blocks. Without the eh-globals swap,
// A leaving its catch would pop B's exception from the shared per-thread
// chain and free it while B still uses it.
TEST(CoroutineExceptions, CatchBlocksMayInterleaveAcrossCoroutines) {
    int aBeforeYield = 0;
    int aAfterYield = 0;
    int aFresh = 0;
    int bBeforeYield = 0;
    int bAfterYield = 0;

    Coroutine a = mustMake([&] {
        try {
            throw 1;
        } catch (int const &) {
            aBeforeYield = rethrowCurrentAsInt();
            Coroutine::yield();
            aAfterYield = rethrowCurrentAsInt();
        }
        try {
            throw 11;
        } catch (int const &) {
            aFresh = rethrowCurrentAsInt();
        }
    });
    Coroutine b = mustMake([&] {
        try {
            throw 2;
        } catch (int const &) {
            bBeforeYield = rethrowCurrentAsInt();
            Coroutine::yield();
            bAfterYield = rethrowCurrentAsInt();
        }
    });

    a.resume(); // A suspended inside its catch
    b.resume(); // B suspended inside its catch
    a.resume(); // A leaves its catch, throws/catches again, finishes
    b.resume(); // B must still see its own exception

    EXPECT_EQ(aBeforeYield, 1);
    EXPECT_EQ(aAfterYield, 1);
    EXPECT_EQ(aFresh, 11);
    EXPECT_EQ(bBeforeYield, 2);
    EXPECT_EQ(bAfterYield, 2);
    EXPECT_TRUE(a.isDone());
    EXPECT_TRUE(b.isDone());
}

TEST(CoroutineExceptions, CatchBlockSurvivesMigrationToAnotherThread) {
    int afterMigration = 0;
    Coroutine coroutine = mustMake([&] {
        try {
            throw 5;
        } catch (int const &) {
            Coroutine::yield();
            afterMigration = rethrowCurrentAsInt();
        }
    });
    coroutine.resume();
    std::thread([&] {
        // The worker has its own, empty exception chain; the coroutine must
        // bring its caught exception along.
        coroutine.resume();
    }).join();
    EXPECT_EQ(afterMigration, 5);
}

TEST(CoroutineExceptions, MainThreadCatchBlockIsUnaffectedByCoroutineExceptions) {
    Coroutine coroutine = mustMake([] {
        try {
            throw std::runtime_error("inner");
        } catch (std::runtime_error const &) {
        }
    });
    int seen = 0;
    try {
        throw 9;
    } catch (int const &) {
        coroutine.resume();
        seen = rethrowCurrentAsInt();
    }
    EXPECT_EQ(seen, 9);
}

#endif // STACKFULL_HAS_EXCEPTIONS
