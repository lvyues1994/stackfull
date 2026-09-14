#include <stackfull/coro/detail/ThreadState.h>

#include <stackfull/coro/Fatal.h>
#include <stackfull/coro/detail/Sanitizer.h>

#include <memory>

#include <pthread.h>

// Backend selection. CMake may force one through STACKFULL_TLS_BACKEND_*;
// otherwise avoid `thread_local` where the toolchain implements it with
// emulated TLS (Android before API 29), which costs a function call and a
// table lookup per access.
#if !defined(STACKFULL_TLS_BACKEND_THREAD_LOCAL) && !defined(STACKFULL_TLS_BACKEND_PTHREAD_KEY)
#if defined(__ANDROID__) && defined(__ANDROID_API__) && (__ANDROID_API__ < 29)
#define STACKFULL_TLS_BACKEND_PTHREAD_KEY 1
#else
#define STACKFULL_TLS_BACKEND_THREAD_LOCAL 1
#endif
#endif

namespace stackfull {
namespace coro {
namespace detail {

namespace {

// The ThreadState lives on the heap and dies with its thread through a pthread
// key destructor. Both backends share this; they differ only in how the fast
// path finds the pointer.
pthread_key_t stateKey;
pthread_once_t stateKeyOnce = PTHREAD_ONCE_INIT;
bool stateKeyValid = false;

void destroyThreadState(void *const raw) {
    std::unique_ptr<ThreadState> const owned(static_cast<ThreadState *>(raw));
}

void createStateKey() {
    stateKeyValid = ::pthread_key_create(&stateKey, &destroyThreadState) == 0;
}

ThreadState *createThreadState() noexcept {
    ::pthread_once(&stateKeyOnce, &createStateKey);
    auto state = std::make_unique<ThreadState>();
    state->mainBlock.thread = state.get();
    state->mainBlock.state = CoroutineState::Running;
    tsanAdoptCurrentFiber(state->mainBlock);
    state->current = &state->mainBlock;
    if (stateKeyValid) {
        ::pthread_setspecific(stateKey, state.get());
    }
    return state.release();
}

} // namespace

#if defined(STACKFULL_TLS_BACKEND_THREAD_LOCAL)

ThreadState &currentThreadState() noexcept {
    // Trivial type + constant initializer: no guard variable, no destructor
    // registration — a single TLS load on the fast path.
    static thread_local ThreadState *cached STACKFULL_TLS_MODEL_ATTR = nullptr;
    if (cached == nullptr) {
        cached = createThreadState();
    }
    return *cached;
}

#else

ThreadState &currentThreadState() noexcept {
    ::pthread_once(&stateKeyOnce, &createStateKey);
    STACKFULL_CHECK(stateKeyValid, "stackfull: pthread_key_create failed; cannot locate per-thread state");
    void *const raw = ::pthread_getspecific(stateKey);
    if (raw != nullptr) {
        return *static_cast<ThreadState *>(raw);
    }
    return *createThreadState();
}

#endif

} // namespace detail
} // namespace coro
} // namespace stackfull
