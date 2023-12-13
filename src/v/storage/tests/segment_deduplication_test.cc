// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "gmock/gmock.h"
#include "random/generators.h"
#include "storage/disk_log_impl.h"
#include "storage/key_offset_map.h"
#include "storage/segment_deduplication_utils.h"
#include "storage/segment_utils.h"
#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/test.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/defer.hh>

#include <stdexcept>

using namespace storage;

namespace {
ss::abort_source never_abort;
} // anonymous namespace

// Builds a segment layout:
// [0    9][10   19][20    29]...
void add_segments(
  storage::disk_log_builder& b,
  int num_segs,
  int records_per_seg = 10,
  int start_offset = 0,
  bool mark_compacted = true) {
    auto& disk_log = b.get_disk_log_impl();
    for (int i = 0; i < num_segs; i++) {
        auto offset = start_offset + i * records_per_seg;
        b | add_segment(offset)
          | add_random_batch(
            offset, records_per_seg, maybe_compress_batches::yes);
    }
    for (auto& seg : disk_log.segments()) {
        if (mark_compacted) {
            seg->mark_as_finished_self_compaction();
            seg->mark_as_finished_windowed_compaction();
        }
        if (seg->has_appender()) {
            seg->appender().close().get();
            seg->release_appender();
        }
    }
}

void build_segments(
  storage::disk_log_builder& b,
  int num_segs,
  int records_per_seg = 10,
  int start_offset = 0,
  bool mark_compacted = true) {
    b | start();
    add_segments(b, num_segs, records_per_seg, start_offset, mark_compacted);
}

TEST(FindSlidingRangeTest, TestCollectSegments) {
    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    for (int start = 0; start < 30; start += 5) {
        for (int end = start; end < 30; end += 5) {
            compaction_config cfg(
              model::offset{end}, ss::default_priority_class(), never_abort);
            auto segs = disk_log.find_sliding_range(cfg, model::offset{start});
            if (end - start < 10) {
                // If the compactible range isn't a full segment, we can't
                // compact anything. We only care about full segments.
                ASSERT_EQ(segs.size(), 0);
                continue;
            }
            // We can't compact partial segments so we round the end down to
            // the nearest segment boundary.
            ASSERT_EQ((end - (end % 10) - start) / 10, segs.size())
              << ssx::sformat("{} to {}: {}", start, end, segs.size());
        }
    }
}

TEST(FindSlidingRangeTest, TestCollectExcludesPrevious) {
    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    compaction_config cfg(
      model::offset{30}, ss::default_priority_class(), never_abort);
    auto segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(3, segs.size());
    ASSERT_EQ(segs.front()->offsets().base_offset(), 0);

    // Let's pretend the previous compaction indexed offsets [20, 30).
    // Subsequent compaction should ignore that last segment.
    disk_log.set_last_compaction_window_start_offset(model::offset(20));
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(2, segs.size());
    ASSERT_EQ(segs.front()->offsets().base_offset(), 0);

    disk_log.set_last_compaction_window_start_offset(model::offset(10));
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(1, segs.size());
    ASSERT_EQ(segs.front()->offsets().base_offset(), 0);
}

TEST(FindSlidingRangeTest, TestCollectExcludesOneRecordSegments) {
    storage::disk_log_builder b;
    build_segments(
      b,
      /*num_segs=*/5,
      /*records_per_seg=*/1,
      /*start_offset=*/0,
      /*mark_compacted=*/false);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    compaction_config cfg(
      model::offset{30}, ss::default_priority_class(), never_abort);
    auto segs = disk_log.find_sliding_range(cfg);
    // All segments so far have only one record and shouldn't be eligible for
    // compaction.
    ASSERT_EQ(0, segs.size());
    for (int i = 0; i < 5; i++) {
        auto& seg = disk_log.segments()[i];
        ASSERT_TRUE(seg->finished_self_compaction());
        ASSERT_TRUE(seg->finished_windowed_compaction());
    }

    // Add some segments with multiple records. They should be eligible for
    // compaction and are included in the range.
    add_segments(
      b,
      /*num_segs=*/3,
      /*records_per_seg=*/2,
      /*start_offset=*/6,
      /*mark_compacted=*/false);
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(3, segs.size());
    for (const auto& seg : segs) {
        ASSERT_FALSE(seg->finished_self_compaction()) << seg;
        ASSERT_FALSE(seg->finished_windowed_compaction()) << seg;
    }

    // Adding more segments with one record, the range should include them
    // since they're not at the beginning of the log.
    add_segments(
      b,
      /*num_segs=*/4,
      /*records_per_seg=*/1,
      /*start_offset=*/13,
      /*mark_compacted=*/false);
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(7, segs.size());
    for (const auto& seg : segs) {
        ASSERT_FALSE(seg->finished_self_compaction());
        ASSERT_FALSE(seg->finished_windowed_compaction());
    }
}

TEST(BuildOffsetMap, TestBuildSimpleMap) {
    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    auto& segs = disk_log.segments();
    compaction_config cfg(
      model::offset{30}, ss::default_priority_class(), never_abort);
    probe pb;

    // Self-compact each segment so we're left with compaction indices. This is
    // a requirement to build the offset map.
    for (auto& seg : segs) {
        storage::internal::self_compact_segment(
          seg,
          disk_log.stm_manager(),
          cfg,
          pb,
          disk_log.readers(),
          disk_log.resources(),
          offset_delta_time::yes)
          .get();
    }

    // Build a map, configuring it to hold too little data for even a single
    // segment.
    simple_key_offset_map too_small_map(5);
    ASSERT_THAT(
      [&] {
          build_offset_map(
            cfg,
            segs,
            disk_log.stm_manager(),
            disk_log.resources(),
            disk_log.get_probe(),
            too_small_map)
            .get();
      },
      testing::ThrowsMessage<std::runtime_error>(
        testing::HasSubstr("Couldn't index")));

    // Now configure a map to index some segments.
    simple_key_offset_map partial_map(15);
    auto partial_o = build_offset_map(
                       cfg,
                       segs,
                       disk_log.stm_manager(),
                       disk_log.resources(),
                       disk_log.get_probe(),
                       partial_map)
                       .get();
    ASSERT_GT(partial_o(), 0);

    // Now make it large enough to index all segments.
    simple_key_offset_map all_segs_map(100);
    auto all_segs_o = build_offset_map(
                        cfg,
                        segs,
                        disk_log.stm_manager(),
                        disk_log.resources(),
                        disk_log.get_probe(),
                        all_segs_map)
                        .get();
    ASSERT_EQ(all_segs_o(), 0);
}

TEST(BuildOffsetMap, TestBuildMapWithMissingCompactedIndex) {
    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    auto& segs = disk_log.segments();
    compaction_config cfg(
      model::offset{30}, ss::default_priority_class(), never_abort);
    for (const auto& s : segs) {
        auto idx_path = s->path().to_compacted_index();
        ASSERT_FALSE(ss::file_exists(idx_path.string()).get());
    }

    // Proceed to window compaction without building any compacted indexes.
    // When building the map, we should attempt to rebuild the index if it
    // doesn't exist.
    simple_key_offset_map missing_index_map(100);
    auto o = build_offset_map(
               cfg,
               segs,
               disk_log.stm_manager(),
               disk_log.resources(),
               disk_log.get_probe(),
               missing_index_map)
               .get();
    ASSERT_EQ(o(), 0);
    ASSERT_EQ(missing_index_map.size(), 30);
    for (const auto& s : segs) {
        auto idx_path = s->path().to_compacted_index();
        ASSERT_TRUE(ss::file_exists(idx_path.string()).get());
    }
}
