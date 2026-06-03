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

#include "hashing/xx.h"
#include "serde/parquet/bloom_filter.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string_view>
#include <unordered_set>

using serde::parquet::bloom_filter;

// Parquet bloom filters mandate xxHash64 (seed=0) specifically — not xxHash3,
// not xxHash32. This known-answer test locks in the correct hash function so
// that accidental substitution is caught immediately.
//
// Test vector: XXH64("", 0 bytes, seed=0) = 0xEF46DB3751D8E999
// Source: https://github.com/Cyan4973/xxHash/blob/dev/cli/xxhsum.c test suite
TEST(BloomFilter, HashKAT) {
    EXPECT_EQ(xxhash_64("", static_cast<size_t>(0)), 0xEF46DB3751D8E999ULL);
}

// Hash a uint64_t index to a realistic xxHash64 value for use in filter tests.
// Using raw sequential integers as "hashes" is a degenerate case: small values
// have h_hi == 0, causing all inserts to set identical bits in block 0.
static uint64_t h(uint64_t i) {
    return xxhash_64(reinterpret_cast<const char*>(&i), sizeof(i));
}

TEST(BloomFilter, NoFalseNegatives) {
    bloom_filter f(1000);
    for (uint64_t i = 0; i < 1000; ++i) {
        f.insert(h(i));
    }
    for (uint64_t i = 0; i < 1000; ++i) {
        EXPECT_TRUE(f.check(h(i)));
    }
}

TEST(BloomFilter, ReasonableFalsePositiveRate) {
    constexpr size_t ndv = 10'000;
    bloom_filter f(ndv);
    for (uint64_t i = 0; i < ndv; ++i) {
        f.insert(h(i));
    }
    // Probe with values that were not inserted. Count false positives.
    size_t false_positives = 0;
    constexpr size_t probe_count = 10'000;
    for (uint64_t i = ndv; i < ndv + probe_count; ++i) {
        if (f.check(h(i))) {
            ++false_positives;
        }
    }
    // Target FPP is ~1%; allow up to 5% to keep the test non-flaky.
    // NOLINTNEXTLINE(*magic-number*)
    EXPECT_LT(false_positives, probe_count / 20);
}

TEST(BloomFilter, Reset) {
    bloom_filter f(100);
    for (uint64_t i = 0; i < 100; ++i) {
        f.insert(h(i));
    }
    f.reset();
    for (uint64_t i = 0; i < 100; ++i) {
        EXPECT_FALSE(f.check(h(i)));
    }
}

TEST(BloomFilter, SerializeNonEmpty) {
    bloom_filter f(1000);
    f.insert(xxhash_64("hello", 5));
    f.insert(xxhash_64("world", 5));

    iobuf out;
    f.serialize(out);

    // Must have written something: Thrift header + bitset.
    EXPECT_GT(out.size_bytes(), f.size_bytes());
}

TEST(BloomFilter, SerializeContainsBitset) {
    bloom_filter f(1000);
    f.insert(42);

    iobuf out;
    f.serialize(out);

    // The total serialized size must be at least the bitset size.
    EXPECT_GE(out.size_bytes(), f.size_bytes());
}

// A filter sized for ndv distinct values but filled with 10× that many
// should have fill_ratio() well above the 0.75 discard threshold used by
// the column writer. This validates that the discard path is reachable for
// realistic over-NDV scenarios.
TEST(BloomFilter, FillRatioExceedsDiscardThresholdWhenOverfull) {
    constexpr size_t ndv = 100;
    bloom_filter f(ndv);
    for (uint64_t i = 0; i < ndv * 10; ++i) {
        f.insert(h(i));
    }
    EXPECT_GT(f.fill_ratio(), 0.75);
}
