#include <stackfull/stack/MmapStackAllocator.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>

#include <sys/mman.h>
#include <unistd.h>

namespace stackfull {
namespace stack {

std::size_t pageSize() noexcept {
    static std::size_t const cached = [] {
        long const value = ::sysconf(_SC_PAGESIZE);
        return value > 0 ? static_cast<std::size_t>(value) : std::size_t{4096};
    }();
    return cached;
}

namespace {

std::size_t roundUpToPage(std::size_t const bytes, std::size_t const page) noexcept {
    return (bytes + page - 1) / page * page;
}

std::error_code lastOsError() noexcept {
    return std::error_code(errno, std::generic_category());
}

int mmapFlags(MmapStackOptions const &options) noexcept {
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#if defined(__linux__) && defined(MAP_STACK)
    // Linux: purely a hint today, but keeps the mapping eligible for future
    // stack-specific kernel policies. QNX gives MAP_STACK different semantics
    // (lazy commit + kernel guard), so it is deliberately not set there yet.
    flags |= MAP_STACK;
#endif
#if defined(MAP_NORESERVE)
    if (options.noReserve) {
        flags |= MAP_NORESERVE;
    }
#endif
    return flags;
}

struct MmapStackAllocatorImpl final : StackAllocator {
    explicit MmapStackAllocatorImpl(MmapStackOptions const &options_) noexcept
        : options(options_), page(pageSize()) {}

    StackAllocation allocate(std::size_t const size) noexcept override {
        std::size_t const usable = roundUpToPage(size, page);
        std::size_t const guard = options.guardPages * page;
        if (usable == 0 or usable > std::numeric_limits<std::size_t>::max() - guard) {
            return StackAllocation{StackView{}, std::make_error_code(std::errc::invalid_argument)};
        }
        std::size_t const total = usable + guard;

        void *const mapping = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, mmapFlags(options), -1, 0);
        if (mapping == MAP_FAILED) {
            return StackAllocation{StackView{}, lastOsError()};
        }
        if (guard != 0 and ::mprotect(mapping, guard, PROT_NONE) != 0) {
            auto const error = lastOsError();
            ::munmap(mapping, total);
            return StackAllocation{StackView{}, error};
        }

        StackView view;
        view.base = static_cast<char *>(mapping) + guard;
        view.size = usable;
        return StackAllocation{view, std::error_code{}};
    }

    // One mapping carved into [guard | stack] slots; each stack is later
    // unmapped on its own, which munmap allows for any page range.
    std::size_t allocateMany(std::size_t const size, StackView *const out, std::size_t const count) noexcept override {
        std::size_t const usable = roundUpToPage(size, page);
        std::size_t const guard = options.guardPages * page;
        if (count == 0 or usable == 0 or usable > std::numeric_limits<std::size_t>::max() - guard) {
            return 0;
        }
        std::size_t const slot = usable + guard;
        if (slot > std::numeric_limits<std::size_t>::max() / count) {
            return 0;
        }
        void *const mapping = ::mmap(nullptr, slot * count, PROT_READ | PROT_WRITE, mmapFlags(options), -1, 0);
        if (mapping == MAP_FAILED) {
            return 0;
        }
        std::size_t done = 0;
        for (; done < count; ++done) {
            char *const start = static_cast<char *>(mapping) + done * slot;
            if (guard != 0 and ::mprotect(start, guard, PROT_NONE) != 0) {
                ::munmap(start, (count - done) * slot);
                break;
            }
            out[done] = StackView{start + guard, usable};
        }
        return done;
    }

    void deallocate(StackView const &stack) noexcept override {
        if (isEmpty(stack)) {
            return;
        }
        std::size_t const guard = options.guardPages * page;
        ::munmap(static_cast<char *>(stack.base) - guard, stack.size + guard);
    }

private:
    MmapStackOptions options;
    std::size_t page;
};

} // namespace

std::unique_ptr<StackAllocator> makeMmapStackAllocator(MmapStackOptions const &options) {
    return std::make_unique<MmapStackAllocatorImpl>(options);
}

} // namespace stack
} // namespace stackfull
