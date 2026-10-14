#pragma once

// stackfull tasks in lexec's sender/receiver model (C++17):
//
//   exec::scheduler sf{scheduler};               // a stackfull Scheduler as a lexec scheduler
//   lexec::schedule(sf) | lexec::then(f)         // f runs inside a task of `scheduler`
//   exec::spawn(sf, fn)                          // fn in a task, as a sender of its result
//   auto values = exec::wait(sndr);              // in a task: park until sndr completes
//
// See README ("sender/receiver") for the rules on stop and exceptions.

#include <stackfull/exec/detail/task_context.hpp>
#include <stackfull/exec/scheduler.hpp>
#include <stackfull/exec/spawn.hpp>
#include <stackfull/exec/wait.hpp>
