#include <stackfull/fcontext/Fcontext.h>

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>

namespace fc = stackfull::fcontext;

namespace {

constexpr std::size_t kStackSize = 64 * 1024;
alignas(16) char stackMemory[kStackSize];

bool isOnTestStack(void const *const address) noexcept {
    auto const p = reinterpret_cast<std::uintptr_t>(address);
    auto const lo = reinterpret_cast<std::uintptr_t>(stackMemory);
    return p >= lo and p < lo + kStackSize;
}

bool isFrameAligned16() noexcept {
    return reinterpret_cast<std::uintptr_t>(__builtin_frame_address(0)) % 16 == 0;
}

// Opaque call target: keeps GCC's -fipa-stack-alignment from calling the probe
// with a reduced alignment, so the probe reflects the ABI state at entry.
using AlignmentProbe = bool (*)();
AlignmentProbe volatile alignmentProbe = &isFrameAligned16;

struct Exchange {
    int received = 0;
    bool onTestStack = false;
    bool frameAligned = false;
};

// Receives an Exchange*, answers 42, then echoes whatever int it is sent next.
void echoEntry(fc::transfer_t transfer) {
    auto *const exchange = static_cast<Exchange *>(transfer.data);
    // The frame address, not a local's address: under ASan address-taken
    // locals live on the fake stack, the frame itself always on the real one.
    exchange->onTestStack = isOnTestStack(__builtin_frame_address(0));
    exchange->frameAligned = alignmentProbe();

    int answer = 42;
    fc::transfer_t const next = fc::jump(transfer.fctx, &answer);

    exchange->received = *static_cast<int const *>(next.data);
    fc::jump(next.fctx, nullptr);
    // A context function must never return; the test never resumes us again.
}

fc::transfer_t addHundredOnTop(fc::transfer_t transfer) {
    *static_cast<int *>(transfer.data) += 100;
    return transfer;
}

} // namespace

TEST(Fcontext, MakeAndJumpRoundTrip) {
    Exchange exchange;
    fc::fcontext_t const ctx = fc::make(stackMemory + kStackSize, kStackSize, &echoEntry);
    ASSERT_NE(ctx, nullptr);

    fc::transfer_t const first = fc::jump(ctx, &exchange);
    EXPECT_TRUE(exchange.onTestStack);
    EXPECT_TRUE(exchange.frameAligned);
    EXPECT_EQ(*static_cast<int const *>(first.data), 42);

    int payload = 7;
    fc::jump(first.fctx, &payload);
    EXPECT_EQ(exchange.received, 7);
}

TEST(Fcontext, OntopRunsOnTargetStackBeforeItContinues) {
    Exchange exchange;
    fc::fcontext_t const ctx = fc::make(stackMemory + kStackSize, kStackSize, &echoEntry);
    fc::transfer_t const suspended = fc::jump(ctx, &exchange);

    int payload = 7;
    fc::ontop(suspended.fctx, &payload, &addHundredOnTop);
    EXPECT_EQ(payload, 107);
    EXPECT_EQ(exchange.received, 107);
}
