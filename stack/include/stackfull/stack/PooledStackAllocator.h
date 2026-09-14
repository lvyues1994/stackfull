#pragma once

#include <stackfull/stack/StackAllocator.h>

#include <cstddef>
#include <memory>

namespace stackfull {
namespace stack {

struct PooledStackOptions {
    // Upper bounds for the per-thread free list. Once either is reached,
    // further deallocations go straight back to the upstream allocator.
    std::size_t maxCachedStacksPerThread = 64;
    std::size_t maxCachedBytesPerThread = std::size_t{32} << 20; // 32 MiB
};

// Decorator: keeps a LIFO free list of stacks *per thread*, bucketed by size,
// in front of `upstream`. Reusing the most recently released stack keeps its
// pages hot in cache and avoids the mmap/munmap pair on the create/destroy
// path. Stacks are not bound to threads: a stack released on thread B after
// being allocated on thread A simply lands in B's cache.
//
// `upstream` is borrowed and must outlive the returned allocator. The pooled
// allocator itself must outlive every thread that used it; destroying it
// drains all thread caches back to `upstream`.
std::unique_ptr<StackAllocator> makePooledStackAllocator(StackAllocator &upstream,
                                                         PooledStackOptions const &options = PooledStackOptions{});

} // namespace stack
} // namespace stackfull
