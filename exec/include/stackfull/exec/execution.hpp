#pragma once

// stackfull tasks in lexec's sender/receiver model (C++17):
//
//   exec::scheduler sf{scheduler};               // a stackfull Scheduler as a lexec scheduler
//   lexec::schedule(sf) | lexec::then(f)         // f runs inside a task of `scheduler`
//   exec::schedule_after(sf, 10ms)               // the same, later
//   exec::spawn(sf, fn)                          // fn in a task, as a sender of its result
//   auto values = exec::wait(sndr);              // in a task: park until sndr completes
//   exec::async_lock(mutex), async_recv(channel), async_read(socket, ...), ...
//                                                // waits as senders, without a task
//
// See README ("sender/receiver") for the rules on stop and exceptions.

#include <stackfull/exec/detail/task_context.hpp>
#include <stackfull/exec/io.hpp>
#include <stackfull/exec/scheduler.hpp>
#include <stackfull/exec/spawn.hpp>
#include <stackfull/exec/sync.hpp>
#include <stackfull/exec/timer.hpp>
#include <stackfull/exec/wait.hpp>
