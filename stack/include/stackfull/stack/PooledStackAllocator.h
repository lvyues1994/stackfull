#pragma once

#include <stackfull/stack/StackAllocator.h>

#include <cstddef>
#include <memory>

namespace stackfull {
namespace stack {

struct PooledStackOptions {
    // Upper bounds for the per-thread free list. Once either is reached,
    // further deallocations go to the shared tier, then to upstream.
    std::size_t maxCachedStacksPerThread = 64;
    std::size_t maxCachedBytesPerThread = std::size_t{32} << 20; // 32 MiB
};

// Decorator with two tiers in front of `upstream`:
//
//   1. a per-thread LIFO free list bucketed by size — the most recently
//      released stack is reused first, its pages still warm;
//   2. a lock-free shared pool that absorbs a thread's overflow and feeds
//      threads whose own cache is empty.
//
// The second tier matters for schedulers: a task's stack is typically
// allocated by the thread that spawned it and released by whichever worker
// ran it, so without it every spawn on a producer thread would fall through
// to mmap.
//
// reserve() adds a third tier: stacks set aside ahead of time (allocated
// from upstream in batches), handed to thread caches a batch at a time. The
// reserved count is also a floor: stacks released while the other tiers
// are full refill it before going back upstream.
//
// `upstream` is borrowed and must outlive the returned allocator. The pooled
// allocator itself must outlive every thread that used it; destroying it
// drains all caches back to `upstream`.
std::unique_ptr<StackAllocator> makePooledStackAllocator(StackAllocator &upstream,
                                                         PooledStackOptions const &options = PooledStackOptions{});

} // namespace stack
} // namespace stackfull
