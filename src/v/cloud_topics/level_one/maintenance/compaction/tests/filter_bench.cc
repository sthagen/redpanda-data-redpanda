/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/bytes.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_filter.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "compaction/tests/simple_reducer.h"
#include "container/chunked_circular_buffer.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <cstddef>
#include <map>
#include <optional>

namespace cloud_topics::l1 {

namespace {

const auto test_ntp = model::ntp(
  model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));

// A semi-compressible payload: a short random block repeated to `size`, so that
// (de)compression performs realistic work rather than operating on either
// trivially-compressible zeros or wholly-incompressible noise.
iobuf gen_value(size_t size) {
    const auto block = random_generators::gen_alphanum_string(64);
    iobuf buf;
    size_t remaining = size;
    while (remaining > 0) {
        const auto step = std::min(remaining, block.size());
        buf.append(block.data(), step);
        remaining -= step;
    }
    return buf;
}

// How records in the generated batch are made eligible for removal, exercising
// the two distinct removal mechanisms of the cloud-topics (L1) filter.
enum class removal_mode : uint8_t {
    // No records removed: a compressed batch is reused verbatim (the path added
    // in 57e62ea1a7). Records are unique, non-tombstone, and nothing is in the
    // removable tombstone set.
    none,
    // A fraction of records share a key indexed at a higher offset and are
    // dropped by key de-duplication, forcing a rebuild + re-compression.
    key_dedup,
    // A fraction of records are tombstones whose offsets fall in the removable
    // tombstone range, forcing a rebuild + re-compression. This path is unique
    // to cloud topics.
    tombstone,
    // Every record is a tombstone but the removable set is empty (e.g.
    // delete.retention.ms has not yet elapsed). Nothing is removed; measures
    // the cost of the per-record removability probe when it cannot fire.
    tombstone_empty_set,
    // Every record is a tombstone and the removable set is non-empty but
    // disjoint from the batch's offset span. Nothing is removed; measures the
    // cost of the per-record probe when a batch-level overlap test could have
    // skipped it.
    tombstone_no_overlap,
};

// A removable set of `n_intervals` singleton intervals, all disjoint from (and
// above) a batch span of `batch_records`. Built once per size and cached: the
// large sets are expensive to construct and must not be rebuilt per bench
// iteration. Single-threaded bench, so a plain static is fine.
const offset_interval_set&
cached_disjoint_set(int n_intervals, int batch_records) {
    static std::map<int, offset_interval_set> cache;
    auto it = cache.find(n_intervals);
    if (it == cache.end()) {
        offset_interval_set s;
        for (int i = 0; i < n_intervals; ++i) {
            // Stride by 2 with singleton intervals so they don't coalesce.
            const auto base = kafka::offset(batch_records + 10 + (i * 2));
            s.insert(base, base);
        }
        it = cache.emplace(n_intervals, std::move(s)).first;
    }
    return it->second;
}

} // namespace

// Exercises `cloud_topics::l1::compaction_filter` over a single batch. The
// `mode`/`removal_frac` parameters select which removal mechanism (if any) is
// triggered; `none` is the optimization-under-test path.
class l1_compaction_filter_bench {
public:
    ss::future<size_t> bench(
      model::compression codec,
      int n_records,
      size_t value_size,
      removal_mode mode,
      double removal_frac,
      int n_removable_intervals = 0) {
        const int n_removed = static_cast<int>(n_records * removal_frac);
        const bool all_tombstones = mode == removal_mode::tombstone_empty_set
                                    || mode
                                         == removal_mode::tombstone_no_overlap;

        // Build and (optionally) compress the input batch outside the timed
        // region; we only want to measure the filter pass.
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        for (int i = 0; i < n_records; ++i) {
            // Tombstone records carry a null value.
            const bool tombstone
              = all_tombstones
                || (mode == removal_mode::tombstone && i < n_removed);
            std::optional<iobuf> value = tombstone ? std::nullopt
                                                   : std::optional<iobuf>(
                                                       gen_value(value_size));
            builder.add_raw_kv(reflection::to_iobuf(i), std::move(value));
        }
        auto batch = std::move(builder).build();
        if (codec != model::compression::none) {
            batch = model::compress_batch_sync(codec, std::move(batch));
        }
        const auto input_bytes = batch.size_bytes();

        // Key de-duplication: index the first `n_removed` keys at an offset
        // strictly higher than any record so the filter drops them.
        compaction::simple_key_offset_map map{};
        if (mode == removal_mode::key_dedup) {
            for (int i = 0; i < n_removed; ++i) {
                auto key = compaction::compaction_key{
                  iobuf_to_bytes(reflection::to_iobuf(i))};
                co_await map.put(key, model::offset(n_records));
            }
        }

        // Tombstone removal: mark the first `n_removed` offsets as removable.
        // For the no-overlap sweep, reference a cached set of the requested
        // size that is disjoint from the batch span.
        offset_interval_set local_removable;
        const offset_interval_set* removable = &local_removable;
        if (mode == removal_mode::tombstone && n_removed > 0) {
            local_removable.insert(
              kafka::offset(0), kafka::offset(n_removed - 1));
        } else if (mode == removal_mode::tombstone_no_overlap) {
            removable = &cached_disjoint_set(n_removable_intervals, n_records);
        }

        chunked_circular_buffer<model::record_batch> output;
        compaction::simple_sink sink(output);
        compaction_filter filter(sink, map, test_ntp, *removable);

        perf_tests::start_measuring_time();
        co_await filter(std::move(batch));
        perf_tests::stop_measuring_time();

        perf_tests::do_not_optimize(output);
        co_return input_bytes;
    }
};

namespace {

constexpr int default_records = 100;
constexpr size_t default_value = 512;

} // namespace

// Optimization path: nothing removed, compressed payload reused verbatim.

PERF_TEST_CN(l1_compaction_filter_bench, zstd_unchanged) {
    return bench(
      model::compression::zstd,
      default_records,
      default_value,
      removal_mode::none,
      0.0);
}

PERF_TEST_CN(l1_compaction_filter_bench, lz4_unchanged) {
    return bench(
      model::compression::lz4,
      default_records,
      default_value,
      removal_mode::none,
      0.0);
}

// Control: key de-duplication forces rebuild + re-compression.

PERF_TEST_CN(l1_compaction_filter_bench, zstd_keys_deduped) {
    return bench(
      model::compression::zstd,
      default_records,
      default_value,
      removal_mode::key_dedup,
      0.5);
}

// Cloud-specific: tombstone-range removal forces rebuild + re-compression.

PERF_TEST_CN(l1_compaction_filter_bench, zstd_tombstones_removed) {
    return bench(
      model::compression::zstd,
      default_records,
      default_value,
      removal_mode::tombstone,
      0.5);
}

// Nothing removed, but every record is a tombstone, so the per-record
// removability probe runs for each one. These two are the cases the empty-set
// guard and the batch-level overlap pre-check target: both should collapse to
// the `zstd_unchanged` cost (all kept -> compressed batch reused verbatim).

PERF_TEST_CN(l1_compaction_filter_bench, zstd_tombstones_empty_set) {
    return bench(
      model::compression::zstd,
      default_records,
      default_value,
      removal_mode::tombstone_empty_set,
      0.0);
}

// No-overlap sweep over removable-set size. Without the guard, every record
// pays a btree lookup against this set; as the set grows past cache, that
// lookup becomes memory-bound. With the guard it is a single per-batch
// overlap() call regardless of set size. The gap widens with set size.

PERF_TEST_CN(l1_compaction_filter_bench, zstd_no_overlap_1k) {
    return bench(
      model::compression::zstd,
      default_records,
      default_value,
      removal_mode::tombstone_no_overlap,
      0.0,
      1'024);
}

PERF_TEST_CN(l1_compaction_filter_bench, zstd_no_overlap_256k) {
    return bench(
      model::compression::zstd,
      default_records,
      default_value,
      removal_mode::tombstone_no_overlap,
      0.0,
      262'144);
}

PERF_TEST_CN(l1_compaction_filter_bench, zstd_no_overlap_8m) {
    return bench(
      model::compression::zstd,
      default_records,
      default_value,
      removal_mode::tombstone_no_overlap,
      0.0,
      8'388'608);
}

} // namespace cloud_topics::l1
