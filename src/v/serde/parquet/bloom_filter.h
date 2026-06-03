/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace serde::parquet {

/// Split-block Bloom filter as specified by the Apache Parquet format.
///
/// Uses xxHash64 (seed=0) as the hash function. One filter instance covers a
/// single column in a single row group. After flush, call reset() to reuse the
/// filter for the next row group without reallocating.
///
/// Serialization format: BloomFilterHeader (Thrift compact) followed
/// immediately by the raw bitset bytes, appended into a caller-provided iobuf.
///
/// See: https://parquet.apache.org/docs/file-format/bloomfilter/
class bloom_filter {
public:
    /// Construct a filter sized for `expected_ndv` distinct values,
    /// targeting ~1% false positive probability.
    explicit bloom_filter(size_t expected_ndv);

    /// Insert a precomputed xxHash64 value into the filter.
    void insert(uint64_t hash);

    /// Check whether a hash may be in the filter. Always returns true if the
    /// value was inserted. May return true for values that were not (false
    /// positive). Intended for use in tests only.
    bool check(uint64_t hash) const;

    /// Reset the filter to empty, keeping the same capacity.
    void reset();

    /// Append [BloomFilterHeader (Thrift compact)][bitset bytes] to `out`.
    void serialize(iobuf& out) const;

    size_t size_bytes() const { return _bitset.size() * sizeof(uint32_t); }

    /// Returns the fraction of bits set. The estimated FPP for the
    /// split-block algorithm is approximately fill_ratio()^8; at the
    /// designed NDV this is ~56%. Callers may discard the filter when
    /// fill_ratio() exceeds a useful threshold (e.g. 0.75 ≈ 10% FPP).
    double fill_ratio() const;

private:
    static size_t num_words_for_ndv(size_t ndv);

    // _bitset is fixed-size, allocated at construction based on expected_ndv:
    // ~9.6 bits per expected distinct value, rounded up to 8-word (256-bit)
    // block boundaries. At ~1% target FPP: 10K NDV → ~12 KiB, 100K NDV →
    // ~120 KiB. There is no dynamic resizing; inserting significantly more
    // distinct values than expected_ndv increases fill_ratio() and degrades
    // FPP. Once fill_ratio() exceeds a useful threshold (e.g. 0.75, ≈ 10%
    // FPP), callers should discard rather than emit the filter.
    std::vector<uint32_t> _bitset;
};

} // namespace serde::parquet
