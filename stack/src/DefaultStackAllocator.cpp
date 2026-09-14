#include <stackfull/stack/DefaultStackAllocator.h>

#include <stackfull/stack/MmapStackAllocator.h>
#include <stackfull/stack/PooledStackAllocator.h>

namespace stackfull {
namespace stack {

StackAllocator &defaultStackAllocator() noexcept {
    // Immortal singletons: released pointers are never deleted (see header).
    static StackAllocator *const upstream = makeMmapStackAllocator().release();
    static StackAllocator *const pooled = makePooledStackAllocator(*upstream).release();
    return *pooled;
}

} // namespace stack
} // namespace stackfull
