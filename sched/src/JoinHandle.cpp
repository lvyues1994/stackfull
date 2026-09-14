#include <stackfull/sched/JoinHandle.h>

#include <stackfull/coro/Config.h>
#include <stackfull/coro/Fatal.h>
#include <stackfull/sched/Parker.h>
#include <stackfull/sched/ThisTask.h>
#include <stackfull/sched/detail/JoinState.h>

#include <atomic>
#include <cstdint>

#if STACKFULL_HAS_EXCEPTIONS
#include <exception>
#endif

namespace stackfull {
namespace sched {

JoinHandle &JoinHandle::operator=(JoinHandle &&other) noexcept {
    if (this != &other) {
        detach();
        state = other.state;
        other.state = nullptr;
    }
    return *this;
}

bool JoinHandle::isDone() const noexcept {
    return state != nullptr and state->done.load(std::memory_order_acquire);
}

void JoinHandle::detach() noexcept {
    if (state != nullptr) {
        state->release();
        state = nullptr;
    }
}

void JoinHandle::join() {
    STACKFULL_CHECK(state != nullptr, "stackfull: join() on an empty JoinHandle");
    detail::JoinState &join = *state;

    if (not join.done.load(std::memory_order_seq_cst)) {
        if (this_task::isTask()) {
            join.taskWaiter = this_task::token();
            join.waiterKind.store(detail::JoinState::kTaskWaiter, std::memory_order_seq_cst);
            while (not join.done.load(std::memory_order_seq_cst)) {
                this_task::park();
            }
        } else {
            join.threadWaiter = makeParker();
            join.waiterKind.store(detail::JoinState::kThreadWaiter, std::memory_order_seq_cst);
            while (not join.done.load(std::memory_order_seq_cst)) {
                join.threadWaiter->park();
            }
        }
    }

#if STACKFULL_HAS_EXCEPTIONS
    std::exception_ptr const escaped = join.exception;
    join.exception = nullptr;
    detach();
    if (escaped) {
        std::rethrow_exception(escaped);
    }
#else
    detach();
#endif
}

} // namespace sched
} // namespace stackfull
