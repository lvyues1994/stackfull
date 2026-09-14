#pragma once

#include <stackfull/coro/Config.h>
#include <stackfull/fcontext/Fcontext.h>
#include <stackfull/sched/detail/Task.h>

// Private to the sched module.

namespace stackfull {
namespace sched {
namespace detail {

#if STACKFULL_HAS_EXCEPTIONS
// ontop function: arrives in a parked task and throws ForcedUnwind at its
// park() call site.
fcontext::transfer_t throwForcedUnwindIntoTask(fcontext::transfer_t transfer);
#endif

// Destroys the body object of a task that never ran or will never resume.
void destroyTaskEntry(Task &task) noexcept;

} // namespace detail
} // namespace sched
} // namespace stackfull
