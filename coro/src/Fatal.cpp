#include <stackfull/coro/Fatal.h>

#include <cstdio>
#include <cstdlib>

namespace stackfull {
namespace coro {

void fatal(char const *const message) noexcept {
    std::fputs(message, stderr);
    std::fputc('\n', stderr);
    std::fflush(stderr);
    std::abort();
}

} // namespace coro
} // namespace stackfull
