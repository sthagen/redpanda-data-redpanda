// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_version.h"
#include "cloud_topics/level_zero/ctp_stm_state.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "random/generators.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace ct = experimental::cloud_topics;

class ct::ctp_stm_state_accessor {
public:
};

using q = ct::ctp_stm_state_accessor;

TEST(ctp_stm_state_death, start_snapshot) {
    ct::ctp_stm_state state;

    auto snapshot_id1 = state.start_snapshot(ct::dl_version(1));
    ASSERT_EQ(snapshot_id1.version, ct::dl_version(1));

    auto snapshot1 = state.read_snapshot(snapshot_id1);
    ASSERT_TRUE(snapshot1.has_value());
    ASSERT_EQ(snapshot1->id, snapshot_id1);

    // This does not exist yet.
    ASSERT_FALSE(
      state.read_snapshot(ct::dl_snapshot_id(ct::dl_version(2))).has_value());

    auto snapshot_id2 = state.start_snapshot(ct::dl_version(2));
    ASSERT_EQ(snapshot_id2.version, ct::dl_version(2));

    auto snapshot2 = state.read_snapshot(snapshot_id2);
    ASSERT_TRUE(snapshot2.has_value());
    ASSERT_EQ(snapshot2->id, snapshot_id2);

    // Starting a snapshot without advancing the version should throw.
    ASSERT_DEATH(
      { state.start_snapshot(ct::dl_version(1)); },
      "Snapshot version can't go backwards. Current snapshot version: 2, new "
      "snapshot version: 1");
}

TEST(ctp_stm_state, start_snapshot) {
    ct::ctp_stm_state state;

    auto snapshot_id0 = state.start_snapshot(ct::dl_version(1));

    auto snapshot_id1 = state.start_snapshot(ct::dl_version(2));

    auto snapshot_id2 = state.start_snapshot(ct::dl_version(3));

    auto snapshot_id3 = state.start_snapshot(ct::dl_version(5));

    auto snapshot0 = state.read_snapshot(snapshot_id0);
    ASSERT_TRUE(snapshot0.has_value());
    ASSERT_EQ(snapshot0->id, snapshot_id0);

    auto snapshot1 = state.read_snapshot(snapshot_id1);
    ASSERT_TRUE(snapshot1.has_value());
    ASSERT_EQ(snapshot1->id, snapshot_id1);

    auto snapshot2 = state.read_snapshot(snapshot_id2);
    ASSERT_TRUE(snapshot2.has_value());
    ASSERT_EQ(snapshot2->id, snapshot_id2);

    auto snapshot3 = state.read_snapshot(snapshot_id3);
    ASSERT_TRUE(snapshot3.has_value());
    ASSERT_EQ(snapshot3->id, snapshot_id3);
}

TEST(ctp_stm_state, remove_snapshots_before) {
    ct::ctp_stm_state state;

    EXPECT_THAT(
      [&]() { state.remove_snapshots_before(ct::dl_version(42)); },
      ThrowsMessage<std::runtime_error>(
        testing::HasSubstr("Attempt to remove snapshots before version 42 but "
                           "no snapshots exist")));

    auto snapshot_id0 = state.start_snapshot(ct::dl_version(1));

    auto snapshot_id1 = state.start_snapshot(ct::dl_version(2));

    auto snapshot_id2 = state.start_snapshot(ct::dl_version(3));

    auto snapshot_id3 = state.start_snapshot(ct::dl_version(5));

    // Test that operation is idempotent.
    for (auto i = 0; i < 3; ++i) {
        state.remove_snapshots_before(ct::dl_version(3));

        ASSERT_FALSE(state.snapshot_exists(snapshot_id0));
        ASSERT_FALSE(state.snapshot_exists(snapshot_id1));
        ASSERT_TRUE(state.snapshot_exists(snapshot_id2));
        ASSERT_TRUE(state.snapshot_exists(snapshot_id3));

        // Retrying the an out-of-date version is an idempotent operation too.
        state.remove_snapshots_before(ct::dl_version(2));
    }

    // It should be impossible to make a call like this because the contract
    // with the callers is that they should first call `start_snapshot` and can
    // call remove_snapshots_before only with the result of the `start_snapshot`
    // call.
    // In case this bug is introduced we want to throw an exception instead of
    // failing silently.
    EXPECT_THAT(
      [&]() { state.remove_snapshots_before(ct::dl_version::max()); },
      ThrowsMessage<std::runtime_error>(testing::HasSubstr(
        "Trying to remove snapshots before an non-existent snapshot")));
}
