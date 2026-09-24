#include "Unwind.h"

#include <stackfull/coro/Config.h>
#include <stackfull/coro/ForcedUnwind.h>
#include <stackfull/coro/detail/Switch.h>
#include <stackfull/sched/detail/Runtime.h>
#include <stackfull/sched/detail/Task.h>
#include <stackfull/sched/detail/Worker.h>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

namespace stackfull {
namespace sched {
namespace detail {

void destroyTaskEntry(Task &task) noexcept {
    if (task.entry != nullptr) {
        task.entry->~Entry();
        task.entry = nullptr;
    }
}

namespace {

// Runs the body and destroys it. Nothing escapes: ForcedUnwind ends the body
// silently; any other exception is parked in the block for releaseTask() to
// hand to the JoinHandle or the ExceptionSink.
void runTaskBody(Task &task) {
#if STACKFULL_HAS_EXCEPTIONS
    try {
        task.entry->run();
        destroyTaskEntry(task);
    } catch (coro::ForcedUnwind const &) {
    } catch (...) {
        task.exception = std::current_exception();
    }
    destroyTaskEntry(task);
#else
    task.entry->run();
    destroyTaskEntry(task);
#endif
}

} // namespace

// First entry of every task. Finishing is a direct handoff like park/yield:
// the next runnable task on this worker (or the dispatcher) continues, and
// releases our stack from its side once we are fully switched out.
void taskEntry(fcontext::transfer_t const transfer) {
    coro::detail::ContextBlock &previous = *static_cast<coro::detail::ContextBlock *>(transfer.data);
    Task &self = static_cast<Task &>(*previous.thread->current);
    coro::detail::onArrival(self, transfer);
    recordWakeLatency(self, *static_cast<Worker *>(self.thread->worker));

    runTaskBody(self);

    Worker &worker = *static_cast<Worker *>(self.thread->worker);
    coro::detail::ContextBlock &next = worker.nextOrDispatcher();
    self.postSwitch = coro::detail::PostSwitchHook{&onTaskFinished, &worker};
    coro::detail::finishAndSwitchTo(self, next);
}

#if STACKFULL_HAS_EXCEPTIONS
fcontext::transfer_t throwForcedUnwindIntoTask(fcontext::transfer_t const transfer) {
    coro::detail::ContextBlock &previous = *static_cast<coro::detail::ContextBlock *>(transfer.data);
    coro::detail::ContextBlock &self = *previous.thread->current;
    coro::detail::onArrival(self, transfer);
    throw coro::ForcedUnwind{};
}
#endif

} // namespace detail
} // namespace sched
} // namespace stackfull
