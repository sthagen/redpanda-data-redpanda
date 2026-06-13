/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "container/interval_map.h"

#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <random>

using imap = interval_map<uint64_t, uint64_t>;

TEST(IntervalMap, InsertZeroLengthInterval) {
    imap map;
    for (unsigned int i = 0; i < 10; ++i) {
        // it doesn't matter where the interval starts
        const auto res = map.insert({i, 0}, 0);
        EXPECT_EQ(res, std::make_pair(map.end(), false));
    }
}

TEST(IntervalMap, InsertIntoEmptyMap) {
    for (unsigned int i = 0; i < 10; ++i) {
        for (unsigned int len = 1; len < 10; ++len) {
            imap map;
            // start/length don't matter it should always succeed
            auto res = map.insert({i, len}, 0);
            EXPECT_NE(res.first, map.end());
            EXPECT_TRUE(res.second);
        }
    }
}

TEST(IntervalMap, Size) {
    imap map;
    EXPECT_EQ(map.size(), 0);

    EXPECT_TRUE(map.insert({0, 10}, 0).second);
    EXPECT_EQ(map.size(), 1);

    EXPECT_TRUE(map.insert({10, 10}, 0).second);
    EXPECT_EQ(map.size(), 2);

    map.erase(map.begin());
    EXPECT_EQ(map.size(), 1);

    map.erase(map.begin());
    EXPECT_EQ(map.size(), 0);
}

TEST(IntervalMap, InsertOverlapRejected) {
    imap map;
    EXPECT_TRUE(map.insert({0, 10}, 0).second);

    for (unsigned int i = 0; i < 10; ++i) {
        const auto res = map.insert({i, 10}, 0);
        EXPECT_EQ(res.first, map.find(0));
        EXPECT_FALSE(res.second);
    }
}

TEST(IntervalMap, InsertRightAbut) {
    imap map;
    EXPECT_TRUE(map.insert({0, 10}, 0).second);

    const auto res = map.insert({10, 10}, 0);
    EXPECT_NE(res.first, map.find(0));
    EXPECT_EQ(res.first, map.find(10));
    EXPECT_TRUE(res.second);
}

TEST(IntervalMap, InsertLeftAbut) {
    imap map;
    EXPECT_TRUE(map.insert({10, 10}, 0).second);

    const auto res = map.insert({0, 10}, 0);
    EXPECT_EQ(res.first, map.find(0));
    EXPECT_NE(res.first, map.find(10));
    EXPECT_TRUE(res.second);
}

TEST(IntervalMap, InsertOverlapWithNoGapRejected) {
    imap map;

    // [0, 10) [10, 20) [20, 21)
    EXPECT_TRUE(map.insert({0, 10}, 0).second);
    EXPECT_TRUE(map.insert({10, 10}, 0).second);
    EXPECT_TRUE(map.insert({20, 1}, 0).second);

    // overlap first rejected
    for (unsigned int i = 0; i < 10; ++i) {
        auto res = map.insert({i, 1}, 0);
        EXPECT_EQ(res.first, map.find(0));
        EXPECT_FALSE(res.second);
    }

    // overlap second rejected
    for (unsigned int i = 10; i < 20; ++i) {
        auto res = map.insert({i, 1}, 0);
        EXPECT_EQ(res.first, map.find(10));
        EXPECT_FALSE(res.second);
    }

    // overlap third rejected
    auto res = map.insert({20, 1}, 0);
    EXPECT_EQ(res.first, map.find(20));
    EXPECT_FALSE(res.second);
}

TEST(IntervalMap, InsertWithSparseOverlaps) {
    imap map;

    // [0, 10) [20, 30)
    EXPECT_TRUE(map.insert({0, 10}, 0).second);
    EXPECT_TRUE(map.insert({20, 10}, 0).second);

    // overlap left rejected
    for (unsigned int i = 0; i < 10; ++i) {
        auto res = map.insert({i, 1}, 0);
        EXPECT_EQ(res.first, map.find(0));
        EXPECT_FALSE(res.second);
    }

    // intervals in between can be inserted
    for (unsigned int i = 10; i < 20; ++i) {
        auto res = map.insert({i, 1}, 0);
        EXPECT_EQ(res.first, map.find(i));
        EXPECT_NE(res.first, map.find(0));
        EXPECT_NE(res.first, map.find(20));
        EXPECT_TRUE(res.second);
    }

    // overlap right rejected
    for (unsigned int i = 20; i < 30; ++i) {
        auto res = map.insert({i, 1}, 0);
        EXPECT_EQ(res.first, map.find(20));
        EXPECT_FALSE(res.second);
    }

    // intervals to the right can be inserted
    const auto res = map.insert({30, 1}, 0);
    EXPECT_EQ(res.first, map.find(30));
    EXPECT_TRUE(res.second);
    for (int i = 0; i < 30; ++i) {
        EXPECT_NE(res.first, map.find(i));
    }
}

TEST(IntervalMap, Overlaps) {
    imap map;
    // An empty map overlaps nothing.
    EXPECT_FALSE(map.overlaps({0, 10}));

    // [10, 20) [30, 40)
    EXPECT_TRUE(map.insert({10, 10}, 0).second);
    EXPECT_TRUE(map.insert({30, 10}, 0).second);

    // A zero-length interval never overlaps.
    EXPECT_FALSE(map.overlaps({15, 0}));

    // Exact, contained, and partial overlaps of an interval all report true.
    EXPECT_TRUE(map.overlaps({10, 10}));
    EXPECT_TRUE(map.overlaps({12, 3}));
    EXPECT_TRUE(map.overlaps({5, 6}));  // [5, 11) reaches into [10, 20)
    EXPECT_TRUE(map.overlaps({19, 5})); // [19, 24) reaches into [10, 20)

    // Abutting-but-disjoint, the gap between intervals, and past the end do not
    // overlap.
    EXPECT_FALSE(map.overlaps({0, 10}));  // [0, 10) abuts [10, 20)
    EXPECT_FALSE(map.overlaps({20, 10})); // [20, 30) sits in the gap
    EXPECT_FALSE(map.overlaps({40, 10})); // [40, 50) past the end

    // A range spanning the gap overlaps both intervals.
    EXPECT_TRUE(map.overlaps({0, 50}));
    EXPECT_TRUE(map.overlaps({19, 12})); // [19, 31) touches both

    // overlaps() does not mutate the map.
    EXPECT_EQ(map.size(), 2);
}

TEST(IntervalMap, FindInEmptyMapReturnsEnd) {
    const imap map;
    EXPECT_EQ(map.find(0), map.end());
}

TEST(IntervalMap, FindPastLast) {
    imap map;
    const auto res = map.insert({0, 10}, 0);
    EXPECT_EQ(map.find(8), res.first);
    EXPECT_EQ(map.find(9), res.first);
    EXPECT_EQ(map.find(10), map.end());
    EXPECT_EQ(map.find(11), map.end());
}

TEST(IntervalMap, FindExactStartOffset) {
    imap map;
    EXPECT_TRUE(map.insert({0, 10}, 11).second);
    EXPECT_TRUE(map.insert({20, 5}, 12).second);
    EXPECT_EQ(map.find(0)->second, 11);
    EXPECT_EQ(map.find(20)->second, 12);
}

TEST(IntervalMap, FindBeforeFirstReturnsEnd) {
    imap map;
    EXPECT_TRUE(map.insert({2, 10}, 33).second);
    EXPECT_EQ(map.find(0), map.end());
    EXPECT_EQ(map.find(1), map.end());
}

TEST(IntervalMap, FindMiddleNoGap) {
    imap map;
    // [0, 10) [10, 20) [20, 30)
    EXPECT_TRUE(map.insert({0, 10}, 3).second);
    EXPECT_TRUE(map.insert({10, 10}, 4).second);
    EXPECT_TRUE(map.insert({20, 10}, 5).second);
    EXPECT_EQ(map.find(1)->second, 3);
    EXPECT_EQ(map.find(9)->second, 3);
    EXPECT_EQ(map.find(11)->second, 4);
    EXPECT_EQ(map.find(19)->second, 4);
    EXPECT_EQ(map.find(21)->second, 5);
    EXPECT_EQ(map.find(29)->second, 5);
}

TEST(IntervalMap, FindMiddleWithGap) {
    imap map;
    // [0, 10) [20, 30) [40, 50)
    EXPECT_TRUE(map.insert({0, 10}, 3).second);
    EXPECT_TRUE(map.insert({20, 10}, 4).second);
    EXPECT_TRUE(map.insert({40, 10}, 5).second);
    EXPECT_EQ(map.find(1)->second, 3);
    EXPECT_EQ(map.find(9)->second, 3);
    EXPECT_EQ(map.find(10), map.end());
    EXPECT_EQ(map.find(19), map.end());
    EXPECT_EQ(map.find(21)->second, 4);
    EXPECT_EQ(map.find(29)->second, 4);
    EXPECT_EQ(map.find(30), map.end());
    EXPECT_EQ(map.find(39), map.end());
}

TEST(IntervalMap, BeginEndAreEqualInEmptyMap) {
    const imap map;
    EXPECT_EQ(map.begin(), map.end());
}

TEST(IntervalMap, Empty) {
    imap map;
    EXPECT_TRUE(map.empty());
    EXPECT_TRUE(map.insert({0, 10}, 0).second);
    EXPECT_FALSE(map.empty());
}

TEST(IntervalMap, Erase) {
    // erase 1 becomes empty
    {
        imap map;
        auto res = map.insert({0, 10}, 0);
        EXPECT_FALSE(map.empty());
        auto next = map.erase(res.first);
        EXPECT_TRUE(map.empty());
        EXPECT_EQ(next, map.end());
    }

    // erase 2 becomes empty
    {
        imap map;
        EXPECT_TRUE(map.insert({0, 10}, 0).second);
        EXPECT_TRUE(map.insert({10, 10}, 0).second);
        EXPECT_FALSE(map.empty());

        const auto it1 = map.find(10);
        auto next = map.erase(it1);
        EXPECT_FALSE(map.empty());
        EXPECT_EQ(next, map.end());

        const auto it0 = map.find(0);
        next = map.erase(it0);
        EXPECT_TRUE(map.empty());
        EXPECT_EQ(next, map.end());
    }

    // erase returns non-end next
    {
        imap map;
        EXPECT_TRUE(map.insert({0, 10}, 0).second);
        EXPECT_TRUE(map.insert({10, 10}, 0).second);
        EXPECT_FALSE(map.empty());

        const auto it0 = map.find(0);
        auto next = map.erase(it0);
        EXPECT_FALSE(map.empty());
        EXPECT_EQ(next, map.begin());
        EXPECT_NE(next, map.end());

        const auto it1 = map.find(10);
        next = map.erase(it1);
        EXPECT_TRUE(map.empty());
        EXPECT_EQ(next, map.end());
    }
}

namespace {
/*
 * A vector<pair<start, size>> describes a set of intervals, which we call a
 * interval map spec. This function produces a set of these specs with
 * varying size intervals, and varying size gaps between the intervals.
 */
std::pair<
  std::random_device::result_type,
  std::vector<std::vector<std::pair<uint64_t, uint64_t>>>>
interval_map_test_specs() {
    static constexpr auto max_interval_size = 20;
    static constexpr auto max_set_size = 4;

    std::random_device rd;
    const auto seed = rd();
    std::mt19937 gen(seed);
    std::uniform_int_distribution<size_t> dist(1, max_interval_size);

    // single interval map specs
    std::vector<std::vector<std::vector<std::pair<uint64_t, uint64_t>>>> specs;
    specs.emplace_back();
    for (int gap = 0; gap <= 3; ++gap) {
        specs.back().push_back({std::make_pair(gap, dist(gen))});
    }

    // build n+1 interval map specs
    for (int set_size = 2; set_size <= max_set_size; ++set_size) {
        std::vector<std::vector<std::pair<uint64_t, uint64_t>>>
          j_interval_specs;
        for (const auto& i_interval_spec : specs.back()) {
            for (int gap = 0; gap <= 3; ++gap) {
                auto tmp = i_interval_spec;
                tmp.emplace_back(
                  tmp.back().first + tmp.back().second + gap, dist(gen));
                j_interval_specs.push_back(tmp);
            }
        }
        specs.push_back(j_interval_specs);
    }

    // flatten results
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> res;
    for (const auto& n_interval_specs : specs) {
        for (const auto& spec : n_interval_specs) {
            res.push_back(spec);
        }
    }

    return {seed, res};
}
} // namespace

TEST(IntervalMap, RandomIntervals) {
    const auto [seed, specs] = interval_map_test_specs();
    for (auto spec : specs) {
        // build map from the test spec
        uint64_t next_value{};
        imap map;
        std::vector<uint64_t> values;
        for (auto [offset, size] : spec) {
            values.push_back(++next_value);
            EXPECT_TRUE(map.insert({offset, size}, values.back()).second);
        }

        // helper to find the value index for a target offset
        auto value_index = [&](uint64_t target) -> std::optional<int> {
            for (size_t i = 0; i < spec.size(); ++i) {
                auto [offset, size] = spec[i];
                if (offset <= target && target < (offset + size)) {
                    return i;
                }
            }
            return std::nullopt;
        };

        // end of the offset range of the test spec
        const auto range_end = spec.back().first + spec.back().second;

        // for each offset from 0..range+3 check that the map finds the same
        // value for the interval as the helper above which operates on the test
        // spec.
        for (size_t offset = 0; offset <= (range_end + 3); ++offset) {
            auto it = map.find(offset);
            auto v_idx = value_index(offset);
            if (v_idx.has_value()) {
                ASSERT_NE(it, map.end());
                EXPECT_EQ(it->second, values.at(v_idx.value()));
            } else {
                EXPECT_EQ(it, map.end());
            }
        }

        seastar::maybe_yield().get();
    }
}

TEST(IntervalMap, AssignExactMatchOnly) {
    imap map;
    EXPECT_TRUE(map.insert({0, 10}, 1).second);
    EXPECT_TRUE(map.insert({10, 10}, 2).second);

    // Exact bounds: updates in place and leaves the interval set unchanged.
    EXPECT_TRUE(map.assign({0, 10}, 100));
    EXPECT_EQ(map.find(0)->second, 100);
    EXPECT_EQ(map.size(), 2);
    // The neighbouring interval is untouched.
    EXPECT_EQ(map.find(10)->second, 2);

    // Same base but wrong length (subset / superset) does not match.
    EXPECT_FALSE(map.assign({0, 5}, 7));
    EXPECT_FALSE(map.assign({0, 20}, 7));
    EXPECT_EQ(map.find(0)->second, 100);

    // An offset inside an interval that is not its base does not match.
    EXPECT_FALSE(map.assign({5, 5}, 7));
    EXPECT_EQ(map.find(0)->second, 100);

    // Absent range and zero length are no-ops.
    EXPECT_FALSE(map.assign({100, 10}, 7));
    EXPECT_FALSE(map.assign({0, 0}, 7));
    EXPECT_EQ(map.size(), 2);
}
