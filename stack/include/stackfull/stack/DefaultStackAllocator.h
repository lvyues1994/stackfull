#pragma once

#include <stackfull/stack/StackAllocator.h>

namespace stackfull {
namespace stack {

// Process-wide default: a pooled allocator in front of an mmap allocator with
// one guard page. Created on first use and intentionally never destroyed so
// threads that outlive static destruction can still release their stacks.
StackAllocator &defaultStackAllocator() noexcept;

} // namespace stack
} // namespace stackfull
