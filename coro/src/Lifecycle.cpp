#include "Lifecycle.h"

#include <stackfull/coro/Config.h>
#include <stackfull/coro/Coroutine.h>
#include <stackfull/coro/Fatal.h>
#include <stackfull/coro/ForcedUnwind.h>
#include <stackfull/coro/detail/Sanitizer.h>
#include <stackfull/coro/detail/Switch.h>
#include <stackfull/coro/detail/ThreadState.h>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

namespace stackfull {
namespace coro {
namespace detail {

void destroyEntry(ContextBlock &block) noexcept {
    if (block.entry != nullptr) {
        block.entry->~Entry();
        block.entry = nullptr;
    }
}

namespace {

// Executes the body and destroys it afterwards. In exception builds nothing
// escapes: a ForcedUnwind ends the body silently, anything else is parked in
// the block for the resumer to rethrow.
void runBody(ContextBlock &block) {
#if STACKFULL_HAS_EXCEPTIONS
    try {
        block.entry->run();
        destroyEntry(block);
    } catch (ForcedUnwind const &) {
    } catch (...) {
        block.exception = std::current_exception();
    }
    destroyEntry(block); // catch paths
#else
    block.entry->run();
    destroyEntry(block);
#endif
}

#if STACKFULL_HAS_EXCEPTIONS

// Runs on the suspended coroutine's stack, at its yield() call site.
fcontext::transfer_t throwForcedUnwind(fcontext::transfer_t const transfer) {
    ContextBlock &prev = *static_cast<ContextBlock *>(transfer.data);
    ContextBlock &self = *prev.thread->current;
    onArrival(self, transfer);
    throw ForcedUnwind{};
}

void unwindSuspended(ContextBlock &block) {
    ThreadState &thread = currentThreadState();
    ContextBlock &from = *thread.current;
    STACKFULL_CHECK(&from != &block, "stackfull: a coroutine cannot destroy itself");
    block.resumer = &from;
    switchToOnTop(from, block, &throwForcedUnwind);
    STACKFULL_ASSERT(block.state == CoroutineState::Done);
    // An exception raised while unwinding has no resumer left to observe it.
    block.exception = nullptr;
}

#endif

void releaseStack(ContextBlock &block) noexcept {
    stack::StackView const stack = block.stack;
    stack::StackAllocator *const allocator = block.allocator;
    tsanDestroyFiber(block);
    block.~ContextBlock();
    allocator->deallocate(stack);
}

} // namespace

void contextEntry(fcontext::transfer_t const transfer) {
    ContextBlock &prev = *static_cast<ContextBlock *>(transfer.data);
    ContextBlock &self = *prev.thread->current;
    onArrival(self, transfer);
    runBody(self);
    finishAndSwitchTo(self, *self.resumer);
}

void destroyBlock(ContextBlock &block) {
    switch (block.state) {
    case CoroutineState::Running:
        fatal("stackfull: destroying a running coroutine");
    case CoroutineState::Suspended:
#if STACKFULL_HAS_EXCEPTIONS
        unwindSuspended(block);
#else
        // Without exceptions the body's frames cannot be unwound. Only the
        // body object itself is destroyed; use requestStop() for clean exits.
        STACKFULL_ASSERT(!"stackfull: destroying a suspended coroutine without exception support");
        destroyEntry(block);
#endif
        break;
    case CoroutineState::Created:
        destroyEntry(block);
        break;
    case CoroutineState::Done:
        break;
    }
    releaseStack(block);
}

} // namespace detail
} // namespace coro
} // namespace stackfull
