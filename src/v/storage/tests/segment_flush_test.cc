// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_builder.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/tests/utils/disk_log_builder.h"

#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <optional>

struct segment_flush_fixture : public testing::Test {
    storage::disk_log_builder b;
    ~segment_flush_fixture() override { b.stop().get(); }
};

// Exercises segment::do_flush() when an append is caught mid-batch (header
// copied, records still pending). do_flush() publishes the raw appender write
// cursor (_appender->file_byte_offset()) as the readable file_size(), so the
// readable extent can land past the last complete batch and expose a partial
// trailing batch to readers. That is the root cause of the
// parser_errc::input_stream_not_enough_bytes ("short read") observed during
// raft recovery.
TEST_F(segment_flush_fixture, flush_with_inflight_partial_batch) {
    using namespace storage; // NOLINT
    b | start() | add_segment(0);
    auto& seg = b.get_segment(0);

    // One complete, flushed batch establishes a well-defined committed
    // boundary.
    auto batch = std::move(
                   record_batch_builder(
                     model::record_batch_type::raft_data, model::offset(0))
                     .add_raw_kv(std::nullopt, std::nullopt))
                   .build();
    seg.append(std::move(batch)).get();
    seg.flush().get();
    ASSERT_TRUE(seg.has_appender());

    const auto committed = seg.offsets().get_committed_offset();
    ASSERT_EQ(committed, model::offset(0));
    const auto complete_file_size = seg.file_size();
    ASSERT_GT(complete_file_size, 0u);

    // Mimic do_append caught mid-batch: push raw bytes through the appender so
    // its write cursor moves past the last complete batch WITHOUT completing a
    // batch (dirty_offset stays put).
    constexpr size_t payload_size = 64;
    ss::temporary_buffer<char> partial(payload_size);
    seg.appender().append(partial.get(), partial.size()).get();
    ASSERT_GT(seg.appender().file_byte_offset(), complete_file_size);

    seg.flush().get();

    // Flushing does not advance committed past the last complete batch.
    EXPECT_EQ(seg.offsets().get_committed_offset(), committed);

    // Readable extent stays at the last complete batch boundary.
    EXPECT_EQ(seg.file_size(), complete_file_size);
}
