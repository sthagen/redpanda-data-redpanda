// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

#include "cloud_topics/level_one/maintenance/leveling/leveling_queue.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "random/generators.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

namespace cloud_topics::l1 {

namespace {
leveling_cmp_t by_extent_count() {
    return [](const leveling_job_ptr& a, const leveling_job_ptr& b) {
        return a->range.extent_count < b->range.extent_count;
    };
}
constexpr int64_t range_span = 99;
constexpr int64_t max_base_offset = int64_t{1} << 30;
constexpr size_t max_extent_count = size_t{1} << 20;
} // namespace

// Benchmarks the two-level leveling_queue across `Partitions` partitions, each
// holding `RangesPerPartition` jobs. Jobs are interleaved across partitions so
// pushes alternate between partitions, exercising the addressable outer queue's
// in-place updates.
template<size_t Partitions, size_t RangesPerPartition>
struct leveling_queue_bench {
    static constexpr size_t total = Partitions * RangesPerPartition;
    static constexpr size_t inner_iters = std::max<size_t>(
      1, 10000 / std::max<size_t>(1, total));

    const model::topic_id topic = model::topic_id::create();
    const chunked_vector<leveling_job_ptr> data = make_data();

    leveling_job_ptr mk_job(size_t p) const {
        auto tidp = model::topic_id_partition(
          topic, model::partition_id(static_cast<int32_t>(p)));
        auto ntp = model::ntp(
          model::ns("bench"),
          model::topic("t"),
          model::partition_id(static_cast<int32_t>(p)));
        auto meta = ss::make_lw_shared<log_compaction_meta>(
          tidp, std::move(ntp));
        auto base = random_generators::get_int<int64_t>(0, max_base_offset);
        return ss::make_lw_shared<leveling_job>(
          std::move(meta),
          levelable_range{
            .base_offset = kafka::offset(base),
            .last_offset = kafka::offset(base + range_span),
            .size_bytes = 1,
            .extent_count = random_generators::get_int<size_t>(
              1, max_extent_count),
          },
          metastore::compaction_epoch{0});
    }

    chunked_vector<leveling_job_ptr> make_data() const {
        chunked_vector<leveling_job_ptr> v;
        v.reserve(total);
        for (size_t r = 0; r < RangesPerPartition; ++r) {
            for (size_t p = 0; p < Partitions; ++p) {
                v.push_back(mk_job(p));
            }
        }
        return v;
    }

    leveling_queue make_filled() const {
        leveling_queue q(by_extent_count());
        for (const auto& job : data) {
            q.push(job);
        }
        return q;
    }

    // Build a queue and push every job. Framework times the whole call.
    [[gnu::noinline]] void run_push() {
        leveling_queue q(by_extent_count());
        for (const auto& job : data) {
            q.push(job);
        }
        perf_tests::do_not_optimize(q);
    }

    // Drain a full queue to empty (k-way merge popping the global best).
    [[gnu::noinline]] size_t run_drain() {
        std::vector<leveling_queue> qs;
        qs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            qs.push_back(make_filled());
        }
        perf_tests::start_measuring_time();
        for (auto& q : qs) {
            while (!q.empty()) {
                perf_tests::do_not_optimize(q.top());
                q.pop();
            }
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }

    // Steady-state worker pattern: pop the global best and push a replacement,
    // keeping the queue full. Measures per-dequeue cost in isolation.
    [[gnu::noinline]] size_t run_steady_pop_push() {
        auto q = make_filled();
        perf_tests::start_measuring_time();
        for (size_t i = 0; i < inner_iters; ++i) {
            auto job = q.top();
            q.pop();
            q.push(job);
            perf_tests::do_not_optimize(q);
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }
};

// NOLINTBEGIN(*-macro-*)
#define LEVELING_QUEUE_PERF_TEST(partitions, ranges)                           \
    class leveling_queue_bench_##partitions##_##ranges                         \
      : public leveling_queue_bench<partitions, ranges> {};                    \
    PERF_TEST_F(leveling_queue_bench_##partitions##_##ranges, Push) {          \
        run_push();                                                            \
    }                                                                          \
    PERF_TEST_F(leveling_queue_bench_##partitions##_##ranges, Drain) {         \
        return run_drain();                                                    \
    }                                                                          \
    PERF_TEST_F(leveling_queue_bench_##partitions##_##ranges, SteadyPopPush) { \
        return run_steady_pop_push();                                          \
    }
// NOLINTEND(*-macro-*)

// few partitions, few ranges each
LEVELING_QUEUE_PERF_TEST(16, 4)
// many partitions, few ranges each (the expected shape: lots of CTPs, small
// per-CTP range counts)
LEVELING_QUEUE_PERF_TEST(1024, 8)
LEVELING_QUEUE_PERF_TEST(4096, 8)
// stress: many partitions, more ranges each
LEVELING_QUEUE_PERF_TEST(4096, 32)

} // namespace cloud_topics::l1
