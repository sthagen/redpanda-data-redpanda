// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/partition_manifest.h"
#include "cluster/archival/replica_state_validator.h"
#include "model/fundamental.h"
#include "storage/ntp_config.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/tmp_dir.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

static void
generate_data(storage::disk_log_builder& builder, model::offset base) {
    auto ts = model::timestamp::now();
    // 0..9
    {
        model::test::record_batch_spec spec{
          .offset = base + model::offset{0},
          .count = 10,
          .records = 1,
          .bt = model::record_batch_type::raft_data,
          .timestamp = ts,
        };
        builder.add_random_batch(spec).get();
    }
    // 10..11
    {
        model::test::record_batch_spec spec{
          .offset = base + model::offset{10},
          .count = 1,
          .records = 2,
          .bt = model::record_batch_type::raft_configuration,
          .timestamp = ts,
        };
        builder.add_random_batch(spec).get();
    }
    // 12..20
    {
        model::test::record_batch_spec spec{
          .offset = base + model::offset{12},
          .count = 9,
          .records = 1,
          .bt = model::record_batch_type::raft_data,
          .timestamp = ts,
        };
        builder.add_random_batch(spec).get();
    }
}

TEST(replica_state_validator_test, test_happy_path) {
    temporary_dir tmp_dir("replica_state_validator_test");

    auto path = tmp_dir.get_path();

    auto builder = storage::disk_log_builder{storage::log_config{
      path.native(), 4_KiB, storage::make_sanitized_file_config()}};

    auto ntp = model::ntp{"kafka", "test", 0};
    auto ntp_config = storage::ntp_config{ntp, {path}};

    builder | storage::start(std::move(ntp_config));

    auto defer = ss::defer([&builder] { builder.stop().get(); });

    builder | storage::add_segment(0);

    generate_data(builder, model::offset{0});

    auto log = builder.get_log();

    // Fake partition manifest
    cloud_storage::partition_manifest manifest(
      ntp, model::initial_revision_id(0));
    manifest.add(
      cloud_storage::segment_meta{
        .base_offset = model::offset{0},
        .committed_offset = model::offset{20},
        .delta_offset = model::offset_delta{0},
        .delta_offset_end = model::offset_delta{2},
      });

    archival::replica_state_validator validator(*log, manifest);
    ASSERT_FALSE(validator.has_anomalies());
}

TEST(replica_state_validator_test, test_gap_detected) {
    temporary_dir tmp_dir("replica_state_validator_test");

    auto path = tmp_dir.get_path();

    auto builder = storage::disk_log_builder{storage::log_config{
      path.native(), 4_KiB, storage::make_sanitized_file_config()}};

    auto ntp = model::ntp{"kafka", "test", 0};
    auto ntp_config = storage::ntp_config{ntp, {path}};

    builder | storage::start(std::move(ntp_config));

    auto defer = ss::defer([&builder] { builder.stop().get(); });

    builder | storage::add_segment(100);

    generate_data(builder, model::offset{100});

    auto log = builder.get_log();

    // The gap between the last offset and the start offset of the log
    // should trigger the anomaly
    cloud_storage::partition_manifest manifest(
      ntp, model::initial_revision_id(0));
    manifest.add(
      cloud_storage::segment_meta{
        .base_offset = model::offset{0},
        .committed_offset = model::offset{10},
        .delta_offset = model::offset_delta{0},
        .delta_offset_end = model::offset_delta{2},
      });

    archival::replica_state_validator validator(*log, manifest);
    ASSERT_TRUE(validator.has_anomalies());
    ASSERT_EQ(validator.get_anomalies().size(), 1);
    ASSERT_TRUE(
      validator.get_anomalies().back().type
      == archival::replica_state_anomaly_type::offsets_gap);
}

TEST(replica_state_validator_test, test_delta_mismatch) {
    temporary_dir tmp_dir("replica_state_validator_test");

    auto path = tmp_dir.get_path();

    auto builder = storage::disk_log_builder{storage::log_config{
      path.native(), 4_KiB, storage::make_sanitized_file_config()}};

    auto ntp = model::ntp{"kafka", "test", 0};
    auto ntp_config = storage::ntp_config{ntp, {path}};

    builder | storage::start(std::move(ntp_config));

    auto defer = ss::defer([&builder] { builder.stop().get(); });

    builder | storage::add_segment(0);

    generate_data(builder, model::offset{0});

    auto log = builder.get_log();

    // The manifest's delta-offset value is wrong
    cloud_storage::partition_manifest manifest(
      ntp, model::initial_revision_id(0));
    manifest.add(
      cloud_storage::segment_meta{
        .base_offset = model::offset{0},
        .committed_offset = model::offset{20},
        .delta_offset = model::offset_delta{1},
        .delta_offset_end = model::offset_delta{2},
      });

    archival::replica_state_validator validator(*log, manifest);
    validator.maybe_print_scarry_log_message();
    ASSERT_TRUE(validator.has_anomalies());
    ASSERT_EQ(validator.get_anomalies().size(), 1);
    ASSERT_TRUE(
      validator.get_anomalies().back().type
      == archival::replica_state_anomaly_type::ot_state);
}

TEST(replica_state_validator_test, test_log_truncated) {
    temporary_dir tmp_dir("replica_state_validator_test");

    auto path = tmp_dir.get_path();

    auto builder = storage::disk_log_builder{storage::log_config{
      path.native(), 4_KiB, storage::make_sanitized_file_config()}};

    auto ntp = model::ntp{"kafka", "test", 0};
    auto ntp_config = storage::ntp_config{ntp, {path}};

    builder | storage::start(std::move(ntp_config));

    auto defer = ss::defer([&builder] { builder.stop().get(); });

    builder | storage::add_segment(0);

    generate_data(builder, model::offset{0});

    auto log = builder.get_log();

    // Truncate the log so there is no local data and replica
    // validation becomes impossible (this could happen
    // if the data is removed by retention). We need to check if
    // the ntp_archiver can continue uploading new data.
    log->truncate_prefix(storage::truncate_prefix_config(model::offset(21)))
      .get();

    // The manifest has correct state
    cloud_storage::partition_manifest manifest(
      ntp, model::initial_revision_id(0));

    manifest.add(
      cloud_storage::segment_meta{
        .base_offset = model::offset{12},
        .committed_offset = model::offset{20},
        .delta_offset = model::offset_delta{2},
        .delta_offset_end = model::offset_delta{2},
      });

    archival::replica_state_validator validator(*log, manifest);
    validator.maybe_print_scarry_log_message();
    ASSERT_FALSE(validator.has_anomalies());
}
