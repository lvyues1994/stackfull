#pragma once

#include <cstddef>

// L0 — raw boost.fcontext primitives.
//
// The assembly in ../../../asm is Boost.Context (see asm/PROVENANCE.md) with its
// symbols renamed at compile time. This header is the only place that spells
// the C symbols; everything above L0 uses the stackfull::fcontext wrappers.
//
// Semantics (unchanged from Boost):
//   make(top, size, fn)  Prepare a fresh context on the stack whose highest
//                        address is `top`. The first jump into it calls
//                        fn(transfer_t{from, data}) on that stack. `fn` must
//                        never return.
//   jump(to, data)       Suspend the calling context and continue `to`. Returns
//                        when some context jumps back here; the result carries
//                        the context that jumped to us and the data it passed.
//   ontop(to, data, fn)  Like jump, but on arrival executes fn(transfer_t) on
//                        `to`'s stack before `to` continues; fn's return value
//                        becomes `to`'s jump() result. Used to inject work (for
//                        example an exception) into a suspended context.
//
// None of the switch wrappers is `noexcept` on purpose: a forced unwind is
// thrown *at* the jump call site of the suspended context, and a noexcept
// frame there would turn it into std::terminate.

extern "C" {

typedef void *stackfull_fcontext_t;

struct stackfull_transfer_t {
    stackfull_fcontext_t fctx;
    void *data;
};

stackfull_transfer_t stackfull_jump_fcontext(stackfull_fcontext_t to, void *vp);

stackfull_fcontext_t stackfull_make_fcontext(void *stackTop, std::size_t size,
                                             void (*fn)(stackfull_transfer_t));

stackfull_transfer_t stackfull_ontop_fcontext(stackfull_fcontext_t to, void *vp,
                                              stackfull_transfer_t (*fn)(stackfull_transfer_t));

} // extern "C"

namespace stackfull {
namespace fcontext {

using fcontext_t = ::stackfull_fcontext_t;
using transfer_t = ::stackfull_transfer_t;
using entry_fn = void (*)(transfer_t);
using ontop_fn = transfer_t (*)(transfer_t);

inline transfer_t jump(fcontext_t const to, void *const data) {
    return ::stackfull_jump_fcontext(to, data);
}

inline fcontext_t make(void *const stackTop, std::size_t const size, entry_fn const fn) noexcept {
    return ::stackfull_make_fcontext(stackTop, size, fn);
}

inline transfer_t ontop(fcontext_t const to, void *const data, ontop_fn const fn) {
    return ::stackfull_ontop_fcontext(to, data, fn);
}

} // namespace fcontext
} // namespace stackfull
