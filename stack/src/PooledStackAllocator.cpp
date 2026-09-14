#include <stackfull/stack/PooledStackAllocator.h>

#include <stackfull/stack/MmapStackAllocator.h>
#include <stackfull/queue/BbqQueue.h>

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <memory>
#include <mutex>
#include <vector>

#include <pthread.h>

namespace stackfull {
namespace stack {

namespace {

// Free stacks are chained through a node written at their own top; the pool
// never allocates bookkeeping memory on the allocate/deallocate path.
struct FreeNode {
    FreeNode *next;
};

struct SizeClass {
    std::size_t size = 0;
    FreeNode *head = nullptr;
    std::size_t count = 0;
};

// A thread that uses more distinct stack sizes than this falls through to the
// upstream allocator for the extra sizes.
constexpr std::size_t kMaxSizeClasses = 8;

// Shared tier: lock-free MPMC of whole stacks, any size class mixed. A
// consumer that pops a stack of the wrong size hands it back to its own
// thread cache (or upstream) and tries again a bounded number of times.
using SharedPool = queue::BbqQueue<StackView, 4, 512>;

struct PooledStackAllocatorImpl;

struct ThreadCache {
    PooledStackAllocatorImpl *owner = nullptr;
    SizeClass classes[kMaxSizeClasses];
    std::size_t cachedStacks = 0;
    std::size_t cachedBytes = 0;
};

FreeNode *nodeOf(StackView const &stack) noexcept {
    return reinterpret_cast<FreeNode *>(static_cast<char *>(topOf(stack)) - sizeof(FreeNode));
}

StackView viewOf(FreeNode *const node, std::size_t const size) noexcept {
    StackView view;
    view.base = reinterpret_cast<char *>(node) + sizeof(FreeNode) - size;
    view.size = size;
    return view;
}

struct PooledStackAllocatorImpl final : StackAllocator {
    // The shared pool has alignas(64) members; C++14 `new` guarantees less.
    static void *operator new(std::size_t const size) {
        void *memory = nullptr;
        if (::posix_memalign(&memory, alignof(SharedPool), size) != 0) {
#if defined(__cpp_exceptions) || defined(__EXCEPTIONS)
            throw std::bad_alloc{};
#else
            std::abort();
#endif
        }
        return memory;
    }
    static void operator delete(void *const memory) noexcept { std::free(memory); }

    PooledStackAllocatorImpl(StackAllocator &upstream_, PooledStackOptions const &options_) noexcept
        : upstream(upstream_), options(options_), page(pageSize()) {
        keyValid = ::pthread_key_create(&key, &PooledStackAllocatorImpl::onThreadExit) == 0;
    }

    ~PooledStackAllocatorImpl() override {
        if (not keyValid) {
            return;
        }
        ::pthread_key_delete(key); // no further onThreadExit callbacks for this key
        std::lock_guard<std::mutex> const lock(registryMutex);
        for (ThreadCache *const cache : registry) {
            std::unique_ptr<ThreadCache> const owned(cache); // ownership returns from the pthread key
            drainToUpstream(*owned);
        }
        registry.clear();
        StackView leftover;
        while (shared.pop(leftover) == queue::PopStatus::Ok) {
            upstream.deallocate(leftover);
        }
    }

    StackAllocation allocate(std::size_t const size) noexcept override {
        std::size_t const rounded = roundUpToPage(size);
        ThreadCache *const cache = currentCache();
        if (cache != nullptr) {
            SizeClass *const sizeClass = findClass(*cache, rounded);
            if (sizeClass != nullptr and sizeClass->head != nullptr) {
                return StackAllocation{popFront(*cache, *sizeClass), std::error_code{}};
            }
        }
        StackView fromShared;
        if (takeFromShared(rounded, fromShared, cache)) {
            return StackAllocation{fromShared, std::error_code{}};
        }
        return upstream.allocate(rounded);
    }

    void deallocate(StackView const &stack) noexcept override {
        if (isEmpty(stack)) {
            return;
        }
        ThreadCache *const cache = currentCache();
        if (cache != nullptr and canCache(*cache, stack)) {
            SizeClass *const sizeClass = findOrAddClass(*cache, stack.size);
            if (sizeClass != nullptr) {
                pushFront(*cache, *sizeClass, stack);
                return;
            }
        }
        if (not giveToShared(stack)) {
            upstream.deallocate(stack);
        }
    }

private:
    std::size_t roundUpToPage(std::size_t const bytes) const noexcept {
        return (bytes + page - 1) / page * page;
    }

    bool canCache(ThreadCache const &cache, StackView const &stack) const noexcept {
        return cache.cachedStacks < options.maxCachedStacksPerThread and
               cache.cachedBytes + stack.size <= options.maxCachedBytesPerThread;
    }

    static SizeClass *findClass(ThreadCache &cache, std::size_t const size) noexcept {
        for (SizeClass &sizeClass : cache.classes) {
            if (sizeClass.size == size) {
                return &sizeClass;
            }
        }
        return nullptr;
    }

    static SizeClass *findOrAddClass(ThreadCache &cache, std::size_t const size) noexcept {
        SizeClass *const existing = findClass(cache, size);
        if (existing != nullptr) {
            return existing;
        }
        for (SizeClass &sizeClass : cache.classes) {
            if (sizeClass.size == 0) {
                sizeClass.size = size;
                return &sizeClass;
            }
        }
        return nullptr;
    }

    static void pushFront(ThreadCache &cache, SizeClass &sizeClass, StackView const &stack) noexcept {
        FreeNode *const node = nodeOf(stack);
        node->next = sizeClass.head;
        sizeClass.head = node;
        sizeClass.count += 1;
        cache.cachedStacks += 1;
        cache.cachedBytes += stack.size;
    }

    static StackView popFront(ThreadCache &cache, SizeClass &sizeClass) noexcept {
        FreeNode *const node = sizeClass.head;
        sizeClass.head = node->next;
        sizeClass.count -= 1;
        cache.cachedStacks -= 1;
        cache.cachedBytes -= sizeClass.size;
        return viewOf(node, sizeClass.size);
    }

    bool giveToShared(StackView const &stack) noexcept {
        for (unsigned spins = 0; spins < 8; ++spins) {
            queue::PushStatus const status = shared.push(stack);
            if (status == queue::PushStatus::Ok) {
                return true;
            }
            if (status == queue::PushStatus::Full) {
                return false;
            }
        }
        return false;
    }

    // Pops from the shared tier until a stack of `size` shows up. Stacks of
    // other sizes are parked in the local cache (or returned upstream) so the
    // scan terminates.
    bool takeFromShared(std::size_t const size, StackView &out, ThreadCache *const cache) noexcept {
        for (unsigned attempts = 0; attempts < 4; ++attempts) {
            StackView candidate;
            queue::PopStatus const status = shared.pop(candidate);
            if (status == queue::PopStatus::Empty) {
                return false;
            }
            if (status != queue::PopStatus::Ok) {
                continue; // Busy
            }
            if (candidate.size == size) {
                out = candidate;
                return true;
            }
            SizeClass *const sizeClass =
                (cache != nullptr and canCache(*cache, candidate)) ? findOrAddClass(*cache, candidate.size) : nullptr;
            if (sizeClass != nullptr) {
                pushFront(*cache, *sizeClass, candidate);
            } else {
                upstream.deallocate(candidate);
            }
        }
        return false;
    }

    void drainToUpstream(ThreadCache &cache) noexcept {
        for (SizeClass &sizeClass : cache.classes) {
            while (sizeClass.head != nullptr) {
                upstream.deallocate(popFront(cache, sizeClass));
            }
        }
    }

    ThreadCache *currentCache() noexcept {
        if (not keyValid) {
            return nullptr;
        }
        void *const existing = ::pthread_getspecific(key);
        if (existing != nullptr) {
            return static_cast<ThreadCache *>(existing);
        }
        return createCache();
    }

    ThreadCache *createCache() noexcept {
        auto cache = std::make_unique<ThreadCache>();
        cache->owner = this;
        if (::pthread_setspecific(key, cache.get()) != 0) {
            return nullptr;
        }
        std::lock_guard<std::mutex> const lock(registryMutex);
        registry.push_back(cache.get());
        return cache.release();
    }

    void forgetCache(ThreadCache *const cache) noexcept {
        std::lock_guard<std::mutex> const lock(registryMutex);
        auto const it = std::find(registry.begin(), registry.end(), cache);
        if (it != registry.end()) {
            registry.erase(it);
        }
    }

    static void onThreadExit(void *const raw) {
        std::unique_ptr<ThreadCache> const cache(static_cast<ThreadCache *>(raw));
        PooledStackAllocatorImpl &owner = *cache->owner;
        owner.drainToUpstream(*cache);
        owner.forgetCache(cache.get());
    }

    StackAllocator &upstream;
    PooledStackOptions options;
    std::size_t page;
    pthread_key_t key{};
    bool keyValid = false;
    std::mutex registryMutex;
    std::vector<ThreadCache *> registry;
    SharedPool shared;
};

} // namespace

std::unique_ptr<StackAllocator> makePooledStackAllocator(StackAllocator &upstream,
                                                         PooledStackOptions const &options) {
    return std::make_unique<PooledStackAllocatorImpl>(upstream, options);
}

} // namespace stack
} // namespace stackfull
