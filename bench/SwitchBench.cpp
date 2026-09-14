// Micro benchmarks for the switch and create/destroy paths.
//
//   stackfull_switch_bench [iterations]
//
// Reports nanoseconds per operation. "switch" means one direction, so a
// resume()+yield() round trip counts as two.

#include <stackfull/coro/Coroutine.h>
#include <stackfull/fcontext/Fcontext.h>
#include <stackfull/stack/MmapStackAllocator.h>
#include <stackfull/stack/PooledStackAllocator.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

using namespace stackfull;

namespace {

using Clock = std::chrono::steady_clock;

double nanosPerOp(Clock::time_point const start, Clock::time_point const end, long const ops) {
    auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return static_cast<double>(ns) / static_cast<double>(ops);
}

void report(char const *const name, double const nsPerOp) {
    std::printf("%-44s %8.1f ns\n", name, nsPerOp);
}

// --- raw fcontext ping-pong: the floor we cannot go below ------------------

void rawEntry(fcontext::transfer_t transfer) {
    for (;;) {
        transfer = fcontext::jump(transfer.fctx, nullptr);
    }
}

void benchRawFcontext(long const iterations) {
    auto const allocator = stack::makeMmapStackAllocator();
    auto const allocation = allocator->allocate(64 * 1024);
    fcontext::fcontext_t ctx = fcontext::make(stack::topOf(allocation.stack), allocation.stack.size, &rawEntry);

    auto const start = Clock::now();
    for (long i = 0; i < iterations; ++i) {
        ctx = fcontext::jump(ctx, nullptr).fctx;
    }
    auto const end = Clock::now();
    report("raw fcontext jump (per switch)", nanosPerOp(start, end, iterations * 2));
    allocator->deallocate(allocation.stack);
}

// --- Coroutine resume/yield ping-pong ---------------------------------------

void benchResumeYield(long const iterations) {
    coro::CoroutineCreation created = coro::makeCoroutine([] {
        for (;;) {
            coro::Coroutine::yield();
        }
    });
    coro::Coroutine coroutine = std::move(created.coroutine);

    coroutine.resume(); // warm up: first entry
    auto const start = Clock::now();
    for (long i = 0; i < iterations; ++i) {
        coroutine.resume();
    }
    auto const end = Clock::now();
    report("Coroutine resume+yield (per switch)", nanosPerOp(start, end, iterations * 2));
    // The coroutine is suspended: destruction unwinds it (or drops it without
    // exceptions), which is exactly the cleanup path worth exercising here.
}

// --- create + run to completion + destroy -----------------------------------

void benchCreateDestroy(char const *const name, stack::StackAllocator &allocator, long const iterations) {
    coro::CoroutineOptions options;
    options.allocator = &allocator;
    options.stackSize = 64 * 1024;
    volatile long sink = 0;

    auto const start = Clock::now();
    for (long i = 0; i < iterations; ++i) {
        coro::CoroutineCreation created = coro::makeCoroutine([&sink] { sink = sink + 1; }, options);
        created.coroutine.resume();
    }
    auto const end = Clock::now();
    report(name, nanosPerOp(start, end, iterations));
}

} // namespace

int main(int const argc, char **const argv) {
    long const switchIterations = argc > 1 ? std::atol(argv[1]) : 10'000'000L;
    long const createIterations = switchIterations / 20;

    std::printf("iterations: %ld switches, %ld create/destroy\n", switchIterations, createIterations);
    benchRawFcontext(switchIterations);
    benchResumeYield(switchIterations);

    auto const mmapAllocator = stack::makeMmapStackAllocator();
    auto const pooled = stack::makePooledStackAllocator(*mmapAllocator);
    benchCreateDestroy("create+resume+destroy, pooled allocator", *pooled, createIterations);
    benchCreateDestroy("create+resume+destroy, raw mmap allocator", *mmapAllocator, createIterations / 10);
    return 0;
}
