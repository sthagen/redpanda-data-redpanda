// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "storage/tests/disk_log_builder_fixture.h"

#include <seastar/core/file.hh>

#include <gtest/gtest.h>

namespace {

// Make a batch that is big enough to trigger the indexing threshold.
model::record_batch make_random_batch(
  model::offset o,
  model::timestamp ts,
  int num_records = 1,
  bool big_enough_for_index = true) {
    auto batch_size = storage::segment_index::default_data_buffer_step + 1;
    if (!big_enough_for_index) {
        batch_size = 1024;
    }

    return model::test::make_random_batch(
      model::offset(o),
      num_records,
      false,
      model::record_batch_type::raft_data,
      std::vector<size_t>(num_records, batch_size),
      ts);
}

} // namespace

TEST_F(log_builder_fixture, timequery) {
    using namespace storage; // NOLINT

    b | start();

    // seg0: timestamps 0..99, offset = timestamp
    b | add_segment(0);
    for (auto ts = 0; ts < 100; ts++) {
        auto batch = make_random_batch(model::offset(ts), model::timestamp(ts));
        b | add_batch(std::move(batch));
    }

    // seg1: [(offset, ts)..]
    //  - (100, 100), (101, 100), ... (104, 100)
    //  - (105, 101), (106, 101), ... (109, 101)
    //  ...
    //  - (195, 119), (196, 119), ... (200, 119)
    b | add_segment(100);
    for (auto offset = 100; offset <= 200; offset++) {
        auto ts = 100 + (offset - 100) / 5;
        auto batch = make_random_batch(
          model::offset(offset), model::timestamp(ts));
        b | add_batch(std::move(batch));
    }

    for (const auto& seg : b.get_log_segments()) {
        EXPECT_TRUE(seg->index().batch_timestamps_are_monotonic());
    }

    {
        SCOPED_TRACE(
          "undershoot the timestamp but keep increasing the start offset");
        auto log = b.get_log();
        for (auto start_offset = log->offsets().start_offset;
             start_offset < model::offset(10);
             start_offset++) {
            SCOPED_TRACE(fmt::format("start_offset: {}", start_offset));

            storage::timequery_config config(
              start_offset,
              model::timestamp(0),
              log->offsets().dirty_offset,
              ss::default_priority_class(),
              std::nullopt);

            auto res = log->timequery(config).get();
            EXPECT_TRUE(res);
            EXPECT_EQ(res->time, model::timestamp(start_offset));
            EXPECT_EQ(res->offset, start_offset);
        }
    }

    // in the first segment check that query(ts) -> batch.offset = ts.
    for (auto ts = 0; ts < 100; ts++) {
        auto log = b.get_log();

        storage::timequery_config config(
          log->offsets().start_offset,
          model::timestamp(ts),
          log->offsets().dirty_offset,
          ss::default_priority_class(),
          std::nullopt);

        auto res = log->timequery(config).get();
        EXPECT_TRUE(res);
        EXPECT_EQ(res->time, model::timestamp(ts));
        EXPECT_EQ(res->offset, model::offset(ts));
    }

    // in the second segment
    //   query(100) -> batch.offset = 100
    //   query(101) -> batch.offset = 105
    //   query(105) -> batch.offset = 125
    //   query(106) -> batch.offset = 130
    //   ...
    //   query(120) -> batch.offset = 200
    for (auto ts = 100; ts <= 120; ts++) {
        auto log = b.get_log();

        storage::timequery_config config(
          log->offsets().start_offset,
          model::timestamp(ts),
          log->offsets().dirty_offset,
          ss::default_priority_class(),
          std::nullopt);

        auto offset = (ts - 100) * 5 + 100;

        auto res = log->timequery(config).get();
        EXPECT_TRUE(res);
        EXPECT_EQ(res->time, model::timestamp(ts));
        EXPECT_EQ(res->offset, model::offset(offset));
    }

    b | stop();
}

TEST_F(log_builder_fixture, timequery_multiple_messages_per_batch) {
    using namespace storage; // NOLINT

    b | start();

    b | add_segment(0);

    int num_batches = 10;
    int records_per_batch = 10;

    // Half share the same timestamp.
    for (auto ts = 0; ts < num_batches * records_per_batch / 2;
         ts += records_per_batch) {
        b
          | add_batch(
            model::test::make_random_batch(model::test::record_batch_spec{
              .offset = model::offset(ts),
              // It is sad but we can't properly query for timestamps inside
              // compressed batches.
              .allow_compression = false,
              .count = records_per_batch,
              .timestamp = model::timestamp(ts),
              .all_records_have_same_timestamp = true,
            }));
    }

    // Half have different timestamps.
    for (auto ts = num_batches * records_per_batch / 2;
         ts < num_batches * records_per_batch;
         ts += records_per_batch) {
        auto batch = make_random_batch(
          model::offset(ts), model::timestamp(ts), records_per_batch);
        b | add_batch(std::move(batch));
    }

    for (const auto& seg : b.get_log_segments()) {
        EXPECT_TRUE(seg->index().batch_timestamps_are_monotonic());
    }

    auto log = b.get_log();

    for (auto start_offset = log->offsets().start_offset;
         start_offset < model::offset(num_batches * records_per_batch);
         start_offset++) {
        SCOPED_TRACE(fmt::format("start_offset: {}", start_offset));

        storage::timequery_config config(
          start_offset,
          model::timestamp(0),
          log->offsets().dirty_offset,
          ss::default_priority_class(),
          std::nullopt);

        auto res = log->timequery(config).get();
        EXPECT_TRUE(res);
        EXPECT_EQ(res->time, model::timestamp(start_offset));
        EXPECT_EQ(res->offset, start_offset);
    }

    b | stop();
}

TEST_F(log_builder_fixture, timequery_single_value) {
    using namespace storage; // NOLINT

    b | start();

    // seg0: timestamps [1000...1099], offsets = [0...99]
    b | add_segment(0);
    for (auto offset = 0; offset < 100; ++offset) {
        auto batch = make_random_batch(
          model::offset(offset), model::timestamp(offset + 1000));
        b | add_batch(std::move(batch));
    }

    // ask for time greater than last timestamp f.e 1200
    auto log = b.get_log();
    storage::timequery_config config(
      log->offsets().start_offset,
      model::timestamp(1200),
      log->offsets().dirty_offset,
      ss::default_priority_class(),
      std::nullopt);

    auto empty_res = log->timequery(config).get();
    EXPECT_FALSE(empty_res);

    // ask for 999 it should return first segment
    config.time = model::timestamp(999);

    auto res = log->timequery(config).get();
    EXPECT_TRUE(res);
    EXPECT_EQ(res->time, model::timestamp(1000));
    EXPECT_EQ(res->offset, model::offset(0));
    b | stop();
}

TEST_F(log_builder_fixture, timequery_sparse_index) {
    using namespace storage;

    b | start();

    b | add_segment(0);
    auto batch1 = make_random_batch(model::offset(0), model::timestamp(1000));
    b | add_batch(std::move(batch1));

    // This batch will not be indexed.
    auto batch2 = make_random_batch(
      model::offset(1), model::timestamp(1600), 1, false);
    b | add_batch(std::move(batch2));

    auto batch3 = make_random_batch(model::offset(2), model::timestamp(2000));
    b | add_batch(std::move(batch3));

    const auto& seg = b.get_log_segments().front();
    EXPECT_TRUE(seg->index().batch_timestamps_are_monotonic());
    EXPECT_EQ(seg->index().size(), 2);

    auto log = b.get_log();
    storage::timequery_config config(
      log->offsets().start_offset,
      model::timestamp(1600),
      log->offsets().dirty_offset,
      ss::default_priority_class(),
      std::nullopt);

    auto res = log->timequery(config).get();
    EXPECT_TRUE(res);
    EXPECT_EQ(res->time, model::timestamp(1600));
    EXPECT_EQ(res->offset, model::offset(1));

    b | stop();
}

TEST_F(log_builder_fixture, timequery_one_element_index) {
    using namespace storage;

    b | start();

    b | add_segment(0);

    // This batch doesn't trigger the size indexing threshold,
    // but it's the first one so it gets indexed regardless.
    auto batch = make_random_batch(
      model::offset(0), model::timestamp(1000), 1, false);
    b | add_batch(std::move(batch));

    const auto& seg = b.get_log_segments().front();
    EXPECT_TRUE(seg->index().batch_timestamps_are_monotonic());
    EXPECT_EQ(seg->index().size(), 1);

    auto log = b.get_log();
    storage::timequery_config config(
      log->offsets().start_offset,
      model::timestamp(1000),
      log->offsets().dirty_offset,
      ss::default_priority_class(),
      std::nullopt);

    auto res = log->timequery(config).get();
    EXPECT_TRUE(res);
    EXPECT_EQ(res->time, model::timestamp(1000));
    EXPECT_EQ(res->offset, model::offset(0));

    b | stop();
}

TEST_F(log_builder_fixture, timequery_non_monotonic_log) {
    using namespace storage; // NOLINT

    b | start();

    // seg0:
    // timestamps = [1000, 1001, 1002, 1003, 1002, 1005, 1006, 1007, 1008, 1009]
    // offsets =    [0,    1,    2,    3,    4,    5,    6,    7,    8,    9   ]
    std::vector<std::pair<model::offset, model::timestamp>> batch_spec = {
      {model::offset(0), model::timestamp(1000)},
      {model::offset(1), model::timestamp(1001)},
      {model::offset(2), model::timestamp(1002)},
      {model::offset(3), model::timestamp(1003)},
      {model::offset(4), model::timestamp(1002)},
      {model::offset(5), model::timestamp(1005)},
      {model::offset(6), model::timestamp(1006)},
      {model::offset(7), model::timestamp(1007)},
      {model::offset(8), model::timestamp(1008)},
      {model::offset(9), model::timestamp(1009)},
    };

    b | add_segment(0);
    for (const auto& [offset, ts] : batch_spec) {
        auto batch = make_random_batch(offset, ts);
        b | add_batch(std::move(batch));
    }

    const auto& segs = b.get_log_segments();
    EXPECT_EQ(segs.size(), 1);
    EXPECT_FALSE(segs.front()->index().batch_timestamps_are_monotonic());

    auto log = b.get_log();
    for (const auto& [offset, ts] : batch_spec) {
        storage::timequery_config config(
          log->offsets().start_offset,
          model::timestamp(ts),
          log->offsets().dirty_offset,
          ss::default_priority_class(),
          std::nullopt);

        auto res = log->timequery(config).get();

        if (offset == model::offset(4)) {
            // A timequery will always return from within the
            // first batch that satifies: `batch_max_timestamp >= needle`.
            // So, in this case we pick the first batch with timestamp
            // greater or equal to 1002.
            EXPECT_TRUE(res);
            EXPECT_EQ(res->time, model::timestamp(1002));
            EXPECT_EQ(res->offset, model::offset(2));
        } else {
            EXPECT_TRUE(res);
            EXPECT_EQ(res->time, ts);
            EXPECT_EQ(res->offset, offset);
        }
    }

    // Query for a bogus, really small timestamp.
    // We should return the first element in the log
    storage::timequery_config config(
      log->offsets().start_offset,
      model::timestamp(-5000),
      log->offsets().dirty_offset,
      ss::default_priority_class(),
      std::nullopt);

    auto res = log->timequery(config).get();

    EXPECT_TRUE(res);
    EXPECT_EQ(res->offset, model::offset(0));

    b | stop();
}

TEST_F(log_builder_fixture, timequery_clamp) {
    using namespace storage; // NOLINT

    b | start();

    // The relative time values in `index_state` are clamped at
    // `delta_time_max`. Check that indexed lookups still work in this case.
    std::vector<std::pair<model::offset, model::timestamp>> batch_spec = {
      {model::offset(0), model::timestamp(0)},
      {model::offset(1),
       model::timestamp(storage::offset_time_index::delta_time_max + 1)},
      {model::offset(2),
       model::timestamp(storage::offset_time_index::delta_time_max * 2 + 1)},
    };

    b | add_segment(0);
    for (const auto& [offset, ts] : batch_spec) {
        auto batch = make_random_batch(offset, ts);
        b | add_batch(std::move(batch));
    }

    const auto& segs = b.get_log_segments();
    EXPECT_EQ(segs.size(), 1);
    EXPECT_TRUE(segs.front()->index().batch_timestamps_are_monotonic());

    auto log = b.get_log();
    storage::timequery_config config(
      log->offsets().start_offset,
      model::timestamp(storage::offset_time_index::delta_time_max * 2 + 1),
      log->offsets().dirty_offset,
      ss::default_priority_class(),
      std::nullopt);

    const auto& [expected_offset, expected_ts] = batch_spec.back();
    auto res = log->timequery(config).get();
    EXPECT_TRUE(res);
    EXPECT_EQ(res->time, expected_ts);
    EXPECT_EQ(res->offset, expected_offset);

    b | stop();
}
