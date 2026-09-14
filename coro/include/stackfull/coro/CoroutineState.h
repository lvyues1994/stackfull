#pragma once

#include <cstdint>

namespace stackfull {
namespace coro {

enum class CoroutineState : std::uint8_t {
    Created,   // has a stack and an entry, never resumed
    Running,   // currently executing on some thread
    Suspended, // yielded; may be resumed by any thread
    Done       // entry returned (or threw); stack still owned until the handle dies
};

} // namespace coro
} // namespace stackfull
