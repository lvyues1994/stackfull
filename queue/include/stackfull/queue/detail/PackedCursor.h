#pragma once

#include <cstddef>
#include <cstdint>

namespace stackfull {
namespace queue {
namespace detail {

// Number of bits needed to represent every value in [0, maxValue].
constexpr unsigned bitsToHold(std::size_t const maxValue) noexcept {
    return maxValue == 0 ? 0u : 1u + bitsToHold(maxValue >> 1);
}

constexpr bool isPowerOfTwo(std::size_t const value) noexcept {
    return value != 0 and (value & (value - 1)) == 0;
}

// One machine word holding `version << IndexBits | index`.
//
// The index addresses a slot inside a block; the version counts how many
// times the block has been reused and is what turns a stale read of a
// recycled block into a detectable mismatch instead of an ABA bug. Versions
// wrap silently; every comparison in the queues is an equality test against
// a neighbouring block, so wrap-around is harmless.
template <unsigned IndexBits>
struct PackedCursor {
    static_assert(IndexBits > 0 and IndexBits < 64, "index field must leave room for a version");

    static constexpr std::size_t kIndexMask = (std::size_t{1} << IndexBits) - 1;

    std::size_t raw = 0;

    static constexpr PackedCursor make(std::size_t const version, std::size_t const index) noexcept {
        return PackedCursor{(version << IndexBits) | index};
    }

    constexpr std::size_t index() const noexcept { return raw & kIndexMask; }
    constexpr std::size_t version() const noexcept { return raw >> IndexBits; }

    // Same version, index advanced by `count`. Caller guarantees no overflow
    // into the version field.
    constexpr PackedCursor addIndex(std::size_t const count) const noexcept { return PackedCursor{raw + count}; }

    // Index reset to zero; version incremented when `bump` is true.
    constexpr PackedCursor nextRound(bool const bump) const noexcept {
        return PackedCursor{(raw & ~kIndexMask) + (bump ? (std::size_t{1} << IndexBits) : 0)};
    }

    friend constexpr bool operator==(PackedCursor const a, PackedCursor const b) noexcept { return a.raw == b.raw; }
    friend constexpr bool operator!=(PackedCursor const a, PackedCursor const b) noexcept { return a.raw != b.raw; }
};

// Typical cache line on x86_64 and AArch64. Adjacent-line prefetch on some
// Intel parts pairs lines, so 128 would be the paranoid choice; 64 keeps the
// per-block footprint small and matches the reference implementation.
constexpr std::size_t kCacheLineSize = 64;

} // namespace detail
} // namespace queue
} // namespace stackfull
