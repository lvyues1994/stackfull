#pragma once

#include <stackfull/stack/StackAllocator.h>

#include <cstddef>
#include <memory>

namespace stackfull {
namespace stack {

struct MmapStackOptions {
    // Number of PROT_NONE pages placed below the usable range. 0 disables the
    // guard; a stack overflow then silently corrupts whatever is mapped below.
    std::size_t guardPages = 1;

    // Ask the kernel not to account the mapping against the commit charge
    // (MAP_NORESERVE where available). Useful for very large sparse stacks.
    bool noReserve = false;
};

// One mmap per stack, guard page via mprotect. Sizes are rounded up to the
// system page size. Thread-safe; every call is a system call, so pair it with
// makePooledStackAllocator for high create/destroy rates.
//
// allocateMany() maps a whole batch at once. Each guard page is a separate
// memory mapping (VMA) as far as the kernel is concerned, so every guarded
// stack costs two; with guardPages = 0 a batch is a single mapping, which
// matters where vm.max_map_count is low (65530 on stock Android kernels).
std::unique_ptr<StackAllocator> makeMmapStackAllocator(MmapStackOptions const &options = MmapStackOptions{});

// Runtime page size (sysconf). Cached after the first call.
std::size_t pageSize() noexcept;

} // namespace stack
} // namespace stackfull
