/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/leveling_range_builder.h"
#include "model/fundamental.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

using namespace cloud_topics;
using namespace cloud_topics::l1;

namespace {

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

struct test_extent {
    kafka::offset base;
    kafka::offset last;
    size_t size;
};

struct test_case {
    std::string name;
    std::vector<test_extent> extents;
    size_t min_acceptable;
    std::vector<levelable_range> expected_ranges;
};

class LevelingRangeBuilderTest : public ::testing::TestWithParam<test_case> {};

} // namespace

TEST_P(LevelingRangeBuilderTest, ProducesExpectedRanges) {
    const auto& c = GetParam();
    leveling_range_builder builder{c.min_acceptable};

    for (const auto& ext : c.extents) {
        builder.process_extent(ext.base, ext.last, ext.size);
    }
    auto ranges = std::move(builder).finalize();

    EXPECT_THAT(ranges, ::testing::ElementsAreArray(c.expected_ranges));
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(
  Cases,
  LevelingRangeBuilderTest,
  ::testing::Values(
    // No undersized extents; nothing to level.
    test_case{
      .name = "NoUndersizedExtents",
      .extents = {{0_o, 9_o, 100},
                  {10_o, 19_o, 100},
                  {20_o, 29_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // No extents at all; empty input.
    test_case{
      .name = "Empty",
      .extents = {},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // Two undersized runs: a singleton at obj 1 (discarded, K=1) and
    // an adjacent pair at objs 3->4 (committed, K=2). The healthy obj 2
    // closes the first run; the trailing healthy obj 5 closes the second.
    test_case{
      .name = "SmallSandwichedBetweenLarge",
      .extents
      = {{0_o, 9_o, 100},
         {10_o, 19_o, 2},
         {20_o, 29_o, 100},
         {30_o, 39_o, 15},
         {40_o, 49_o, 2},
         {50_o, 59_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {{.base_offset = 30_o, .last_offset = 49_o, .size_bytes = 17}},
    },
    // Single isolated small surrounded by healthies. A K=1 singleton
    // run that finalize() discards on close.
    test_case{
      .name = "IsolatedSmallSingleton",
      .extents = {{0_o, 9_o, 100},
                  {10_o, 19_o, 2},
                  {20_o, 29_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // Two smalls separated by two healthies. We never level across a
    // healthy sized object, so each small is a K=1 singleton run
    // and neither commits.
    test_case{
      .name = "TwoSmallsSeparatedByHealthies",
      .extents
      = {{0_o, 9_o, 100},
         {10_o, 19_o, 2},
         {20_o, 29_o, 100},
         {30_o, 39_o, 100},
         {40_o, 49_o, 2},
         {50_o, 59_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // The pathological case we naturally avoid: two tiny smalls
    // separated by 8 healthies. A cross-tier merge would rewrite all
    // 804 bytes to save 1 object. As it is, we leave each small as its
    // own K=1 singleton and commit nothing.
    test_case{
      .name = "DistantSmallsAreNotMerged",
      .extents
      = {{0_o, 9_o, 2},
         {10_o, 19_o, 100},
         {20_o, 29_o, 100},
         {30_o, 39_o, 100},
         {40_o, 49_o, 100},
         {50_o, 59_o, 100},
         {60_o, 69_o, 100},
         {70_o, 79_o, 100},
         {80_o, 89_o, 100},
         {90_o, 99_o, 2}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // All extents undersized: one range covers everything, rewritten as
    // one tiny object.
    test_case{
      .name = "AllSmall",
      .extents = {{0_o, 9_o, 2},
                  {10_o, 19_o, 2},
                  {20_o, 29_o, 2}},
      .min_acceptable = 50,
      .expected_ranges = {{.base_offset = 0_o, .last_offset = 29_o, .size_bytes = 6}},
    },
    // Leading healthies are no-ops (no active range to close). Range
    // opens at obj 2, extends to obj 3, and closes on the trailing
    // healthy at obj 4. K=2, commits objs 2..3 (4 bytes).
    test_case{
      .name = "LeadingHealthyExtentsUntouched",
      .extents
      = {{0_o, 9_o, 100},
         {10_o, 19_o, 100},
         {20_o, 29_o, 2},
         {30_o, 39_o, 2},
         {40_o, 49_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {{.base_offset = 20_o, .last_offset = 39_o, .size_bytes = 4}},
    },
    // Smalls separated by a healthy: pure size-tier never bridges across
    // a healthy extent, so each small is a singleton run with no saving.
    // No leveling.
    test_case{
      .name = "SmallsAcrossHealthyAreNotMerged",
      .extents
      = {{0_o, 9_o, 100},
         {10_o, 19_o, 30},
         {20_o, 29_o, 100},
         {30_o, 39_o, 30},
         {40_o, 49_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // A single trailing small with no neighbors. The active range
    // (K=1) reaches finalize() and gets discarded. Guards against
    // finalize() accidentally committing a trivial one-extent run.
    test_case{
      .name = "TrailingSmallAlone",
      .extents = {{0_o, 9_o, 100},
                  {10_o, 19_o, 100},
                  {20_o, 29_o, 2}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // A single leading small followed by healthies. The first healthy
    // closes the K=1 singleton run, which is then discarded.
    test_case{
      .name = "LeadingSmallSingleton",
      .extents = {{0_o, 9_o, 2},
                  {10_o, 19_o, 100},
                  {20_o, 29_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // Threshold below ALL object sizes, nothing eligible.
    test_case{
      .name = "NoEligibleExtents",
      .extents = {{0_o, 9_o, 200},
                  {10_o, 19_o, 200},
                  {20_o, 29_o, 200}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // Two-island scenario: the first three smalls are separated from
    // each other by healthies (no merge), but the last two are adjacent
    // and get consolidated. One range commits.
    test_case{
      .name = "AdjacentSmallsConsolidate",
      .extents = {{0_o, 9_o, 100},
                  {10_o, 19_o, 30},
                  {20_o, 29_o, 100},
                  {30_o, 39_o, 30},
                  {40_o, 49_o, 100},
                  {50_o, 59_o, 100},
                  {60_o, 69_o, 30},
                  {70_o, 79_o, 30},
                  {80_o, 89_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {{.base_offset = 60_o, .last_offset = 79_o, .size_bytes = 60}},
    },
    // Variable extent widths: the algorithm should depend only on size
    // and offset ordering, not on each extent's offset width. Same shape
    // as SmallSandwichedBetweenLarge but with non-uniform offset ranges.
    test_case{
      .name = "NonUniformOffsetWidths",
      .extents
      = {{0_o, 49_o, 100},
         {50_o, 51_o, 2},
         {52_o, 151_o, 100},
         {152_o, 200_o, 15},
         {201_o, 202_o, 2},
         {203_o, 999_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {{.base_offset = 152_o, .last_offset = 202_o, .size_bytes = 17}},
    },
    // Boundary: extents exactly at min_acceptable. Since the undersized
    // check is `< min_acceptable` (strict), 50 is not undersized; no
    // leveling.
    test_case{
      .name = "ExactlyAtMinAcceptable",
      .extents = {{0_o, 9_o, 50},
                  {10_o, 19_o, 50},
                  {20_o, 29_o, 50}},
      .min_acceptable = 50,
      .expected_ranges = {},
    },
    // Boundary: extents just below min_acceptable. All undersized, fully
    // consolidated. Verifies that `size = min_acceptable - 1` IS treated
    // as undersized.
    test_case{
      .name = "JustBelowMinAcceptable",
      .extents = {{0_o, 9_o, 49},
                  {10_o, 19_o, 49},
                  {20_o, 29_o, 49}},
      .min_acceptable = 50,
      .expected_ranges = {{.base_offset = 0_o, .last_offset = 29_o, .size_bytes = 147}},
    },
    // Boundary: mixed sizes at and just below min_acceptable. Only the
    // 49-byte extents are undersized; the 50-byte ones (== min_acceptable)
    // are healthy. Range opens at obj 1, extends to obj 2 (both 49),
    // then closes on the healthy 50 at obj 3. K=2, commits objs 1..2.
    test_case{
      .name = "MixedAtMinAcceptableBoundary",
      .extents
      = {{0_o, 9_o, 50},
         {10_o, 19_o, 49},
         {20_o, 29_o, 49},
         {30_o, 39_o, 50},
         {40_o, 49_o, 100}},
      .min_acceptable = 50,
      .expected_ranges = {{.base_offset = 10_o, .last_offset = 29_o, .size_bytes = 98}},
    }),
  [](const auto& info) { return info.param.name; });
// clang-format on
