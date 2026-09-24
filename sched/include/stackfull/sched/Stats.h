#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace stackfull {
namespace sched {

// Buckets of the wake-latency histogram: bucket 0 counts wakeups that ran
// within 2 us of being made runnable, bucket i (i >= 1) those within
// [2^i, 2^(i+1)) us; the last one is open-ended.
constexpr std::size_t kWakeLatencyBuckets = 24;

struct WorkerStats {
    std::uint64_t tasksSpawned = 0;  // by tasks running on this worker
    std::uint64_t tasksFinished = 0; // on this worker
    std::uint64_t sleeps = 0;        // times it went to sleep for lack of work
    std::uint64_t steals = 0;        // successful steals from other workers
    std::uint64_t yields = 0;        // this_task::yield() calls of its tasks
};

// A snapshot; counters are read one by one while the scheduler runs.
struct SchedulerStats {
    std::size_t liveTasks = 0;
    std::uint64_t tasksSpawned = 0; // including from plain threads
    std::uint64_t tasksFinished = 0;
    std::uint64_t timersFired = 0;
    std::vector<WorkerStats> workers;
    // Only with SchedulerOptions::recordWakeLatency: time from a task being
    // made runnable (spawned, woken) to it running again.
    std::array<std::uint64_t, kWakeLatencyBuckets> wakeLatency{};
};

enum class TaskStatus : std::uint8_t {
    Runnable,  // running, or queued to run
    Parked,    // waiting for a wake
    Waking,    // woken, about to be queued
    Finishing, // done, being released
};

struct TaskInfo {
    std::uint32_t slot = 0;
    char const *name = nullptr; // TaskOptions::name, may be null
    TaskStatus status = TaskStatus::Runnable;
};

} // namespace sched
} // namespace stackfull
