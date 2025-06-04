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

#include "model/fundamental.h"
#include "storage/offset_translator_state.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>

static const model::ntp ntp;

/// Convert to redpanda log offset
constexpr model::offset operator""_rp(unsigned long long o) {
    return model::offset((int64_t)o);
}

constexpr model::offset_delta operator""_do(unsigned long long o) {
    return model::offset_delta((int64_t)o);
}

TEST(OffsetTranslatorStateTest, AddNormal) {
    storage::offset_translator_state state(ntp);

    // base offset
    // segment 1 |[10...gap...14] data [18..gap..19]|
    // segment 2 |20          25..gap...28          |

    // segment 1
    ASSERT_TRUE(state.add_absolute_delta(10_rp, 5));
    ASSERT_EQ(state.last_delta(), 5_do);
    ASSERT_EQ(state.last_gap_offset(), 9_rp);
    state.add_gap(10_rp, 14_rp);
    ASSERT_EQ(state.last_delta(), 10_do);
    ASSERT_EQ(state.last_gap_offset(), 14_rp);
    state.add_gap(18_rp, 19_rp);
    ASSERT_EQ(state.last_delta(), 12_do);
    ASSERT_EQ(state.last_gap_offset(), 19_rp);

    // segment 2
    ASSERT_FALSE(state.add_absolute_delta(20_rp, 12));
    ASSERT_EQ(state.last_delta(), 12_do);
    ASSERT_EQ(state.last_gap_offset(), 19_rp);
    state.add_gap(25_rp, 28_rp);
    ASSERT_EQ(state.last_delta(), 16_do);
    ASSERT_EQ(state.last_gap_offset(), 28_rp);
}

TEST(OffsetTranslatorStateTest, OverlapNonDataBatch) {
    storage::offset_translator_state state(ntp);

    // base offset
    // segment 1 |[10...gap...14] data [18..gap..20]|
    // segment 2 |[18...gap...20] data              |

    // segment 1
    ASSERT_TRUE(state.add_absolute_delta(10_rp, 5));
    ASSERT_EQ(state.last_delta(), 5_do);
    ASSERT_EQ(state.last_gap_offset(), 9_rp);
    state.add_gap(10_rp, 14_rp);
    ASSERT_EQ(state.last_delta(), 10_do);
    ASSERT_EQ(state.last_gap_offset(), 14_rp);
    state.add_gap(18_rp, 20_rp);
    ASSERT_EQ(state.last_delta(), 13_do);
    ASSERT_EQ(state.last_gap_offset(), 20_rp);

    // segment 2
    // This one wouldn't be added to the map because the gap length is 0
    // but the overlapping batch will be removed from it.
    ASSERT_FALSE(state.add_absolute_delta(18_rp, 10));
    ASSERT_EQ(state.last_delta(), 10_do);
    ASSERT_EQ(state.last_gap_offset(), 14_rp);
    state.add_gap(18_rp, 20_rp);
    ASSERT_EQ(state.last_delta(), 13_do);
    ASSERT_EQ(state.last_gap_offset(), 20_rp);
}

TEST(OffsetTranslatorStateTest, Duplicate) {
    storage::offset_translator_state state(ntp);

    // base offset
    // segment 1 |[10...gap...14] data [18..gap..20]|
    // segment 2 |[10...gap...14] data [18..gap..20]|

    // segment 1
    ASSERT_TRUE(state.add_absolute_delta(10_rp, 5));
    ASSERT_EQ(state.last_delta(), 5_do);
    ASSERT_EQ(state.last_gap_offset(), 9_rp);
    state.add_gap(10_rp, 14_rp);
    ASSERT_EQ(state.last_delta(), 10_do);
    ASSERT_EQ(state.last_gap_offset(), 14_rp);
    state.add_gap(18_rp, 20_rp);
    ASSERT_EQ(state.last_delta(), 13_do);
    ASSERT_EQ(state.last_gap_offset(), 20_rp);

    // segment 2
    ASSERT_FALSE(state.add_absolute_delta(10_rp, 5));
    ASSERT_EQ(state.last_delta(), 5_do);
    ASSERT_EQ(state.last_gap_offset(), 9_rp);
    state.add_gap(10_rp, 14_rp);
    ASSERT_EQ(state.last_delta(), 10_do);
    ASSERT_EQ(state.last_gap_offset(), 14_rp);
    state.add_gap(18_rp, 20_rp);
    ASSERT_EQ(state.last_delta(), 13_do);
    ASSERT_EQ(state.last_gap_offset(), 20_rp);
}

TEST(OffsetTranslatorStateTest, Inconsistency1) {
    storage::offset_translator_state state(ntp);

    ASSERT_TRUE(state.add_absolute_delta(10_rp, 9));
    ASSERT_EQ(state.last_delta(), 9_do);
    ASSERT_EQ(state.last_gap_offset(), 9_rp);
    ASSERT_THROW(state.add_absolute_delta(11_rp, 5), std::runtime_error);
}

TEST(OffsetTranslatorStateTest, Inconsistency2) {
    storage::offset_translator_state state(ntp);

    ASSERT_TRUE(state.add_absolute_delta(10_rp, 5));
    ASSERT_EQ(state.last_delta(), 5_do);
    ASSERT_EQ(state.last_gap_offset(), 9_rp);
    state.add_gap(20_rp, 30_rp);
    ASSERT_THROW(state.add_gap(15_rp, 20_rp), std::runtime_error);
}

TEST(OffsetTranslatorStateTest, Case1) {
    storage::offset_translator_state state(ntp);

    ASSERT_TRUE(state.add_absolute_delta(100_rp, 10));
    ASSERT_FALSE(state.add_absolute_delta(110_rp, 10));
}

TEST(OffsetTranslatorStateTest, Case2) {
    storage::offset_translator_state state(ntp);
    ASSERT_TRUE(state.add_absolute_delta(90_rp, 4));
    state.add_gap(95_rp, 100_rp);
    ASSERT_EQ(state.last_delta(), 10_do);
    ASSERT_EQ(state.last_gap_offset(), 100_rp);
    ASSERT_FALSE(state.add_absolute_delta(101_rp, 10));
    ASSERT_EQ(state.last_delta(), 10_do);
    ASSERT_EQ(state.last_gap_offset(), 100_rp);
}
