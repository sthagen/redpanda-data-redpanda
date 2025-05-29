/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "storage/offset_to_filepos.h"
#include "test_utils/archival.h"
#include "test_utils/gtest_exception.h"
#include "test_utils/tmp_dir.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

TEST(OffsetToFileposTest, SearchBeginOffsetNotFound) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};
    for (auto i = 0; i < 2; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 1,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().get_committed_offset());
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_begin_offset_to_file_pos(
                    curr_offset, segment, segment->index().base_timestamp())
                    .get();
    ASSERT_EQ(result.error(), std::errc::invalid_seek);
}

TEST(OffsetToFileposTest, SearchEndOffsetNotFound) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};
    for (auto i = 0; i < 2; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 1,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().get_committed_offset());
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_end_offset_to_file_pos(
                    curr_offset, segment, segment->index().max_timestamp())
                    .get();
    ASSERT_EQ(result.error(), std::errc::invalid_seek);
}

TEST(OffsetToFileposTest, SearchBeginOffsetFound) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};

    std::vector<size_t> positions{};
    for (auto i = 0; i < 5; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 10,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        positions.push_back(b.bytes_written());
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().get_committed_offset());
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_begin_offset_to_file_pos(
                    model::offset{3},
                    segment,
                    segment->index().base_timestamp())
                    .get();
    storage::offset_to_file_pos_result expected{
      model::offset{3},
      positions[2],
      model::timestamp{segment->index().base_timestamp().value() + 3}};
    ASSERT_EQ(result.value(), expected);
}

TEST(OffsetToFileposTest, SearchEndOffsetFound) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};

    std::vector<size_t> positions{};
    for (auto i = 0; i < 5; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 10,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        positions.push_back(b.bytes_written());
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().get_committed_offset());
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_end_offset_to_file_pos(
                    model::offset{3},
                    segment,
                    segment->index().base_timestamp())
                    .get();
    storage::offset_to_file_pos_result expected{
      model::offset{3},
      positions[3], // the end byte offset of the batch containing the needle
      model::timestamp{segment->index().base_timestamp().value() + 3}};
    ASSERT_EQ(result.value(), expected);
}

TEST(OffsetToFileposTest, SearchEndOffsetAllowedToBeMissing) {
    temporary_dir tmp_dir("offset_to_fpos_translate");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });
    b | add_segment(0);
    auto ts = model::timestamp::now();
    auto curr_offset = model::offset{0};
    for (auto i = 0; i < 2; ++i) {
        model::test::record_batch_spec spec{
          .offset = curr_offset,
          .count = 1,
          .records = 1,
          .timestamp = ts,
        };
        b.add_random_batch(spec).get();
        curr_offset = model::next_offset(
          b.get_log_segments().back()->offsets().get_committed_offset());
        ts = model::timestamp{ts.value() + 1};
    }

    auto segment = b.get_log_segments().back();
    auto result = convert_end_offset_to_file_pos(
                    curr_offset,
                    segment,
                    segment->index().max_timestamp(),
                    should_fail_on_missing_offset::no)
                    .get();
    storage::offset_to_file_pos_result expected{
      model::offset{1},
      segment->size_bytes(),
      model::timestamp{segment->index().max_timestamp().value()}};
    ASSERT_EQ(result.value(), expected);
}
