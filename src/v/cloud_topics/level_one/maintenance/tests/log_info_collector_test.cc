/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/level_one/maintenance/log_info_collector.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduling_policies.h"
#include "cluster/topic_configuration.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/random_batch.h"

#include <gtest/gtest.h>

using namespace cloud_topics;

class LogInfoCollectorTestFixture : public l1::l1_reader_fixture {};

// A fake topic config provider which always returns a value.
class fake_cfg_provider : public l1::topic_cfg_provider {
public:
    std::optional<std::reference_wrapper<const cluster::topic_configuration>>
    get_topic_cfg(model::topic_namespace_view) const final {
        return _cfg;
    }

private:
    cluster::topic_configuration _cfg{};
};

// A fake offset provider which always returns kafka::offset::max().
class fake_offset_provider : public l1::max_compactible_offset_provider {
public:
    ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const final {
        co_return;
    }
};

TEST_F(LogInfoCollectorTestFixture, TestInfoCollector) {
    auto cfg_provider = std::make_unique<fake_cfg_provider>();
    auto offset_provider = std::make_unique<fake_offset_provider>();
    l1::log_info_collector log_info_collector(
      &_metastore, std::move(cfg_provider), std::move(offset_provider));
    std::vector<std::pair<model::ntp, model::topic_id_partition>> ntidps;
    const auto topic_names = {"topic_a", "topic_b", "topic_c"};
    const auto num_topics = topic_names.size();
    for (const auto& topic : topic_names) {
        ntidps.push_back(make_ntidp(topic));
    }

    std::vector<tidp_batches_t> tidp_batches;
    l1::log_set_t logs;
    l1::log_compaction_queue cached_metadata(
      [](
        const l1::log_compaction_meta_ptr& a,
        const l1::log_compaction_meta_ptr& b) { return a->ntp < b->ntp; });
    l1::log_list_t logs_list;
    for (const auto& [ntp, tidp] : ntidps) {
        auto [it, success] = logs.emplace(
          ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp));
        logs_list.push_back(*it->get());
        auto batches
          = model::test::make_random_batches(model::offset{0}, 10).get();
        tidp_batches.emplace_back(tidp, std::move(batches));
    }

    make_l1_objects(std::move(tidp_batches)).get();
    log_info_collector.collect_compaction_info(logs, logs_list, cached_metadata)
      .get();
    ASSERT_EQ(cached_metadata.size(), num_topics);
    while (!cached_metadata.empty()) {
        auto sample = cached_metadata.top();
        cached_metadata.pop();
        ASSERT_TRUE(sample->compaction.info_and_ts.has_value());
        ASSERT_FLOAT_EQ(sample->compaction.info_and_ts->info.dirty_ratio, 1.0);
        ASSERT_TRUE(
          sample->compaction.info_and_ts->info.earliest_dirty_ts.has_value());
    }
}

TEST_F(LogInfoCollectorTestFixture, TestSampleLevelingInfo) {
    auto cfg_provider = std::make_unique<fake_cfg_provider>();
    auto offset_provider = std::make_unique<fake_offset_provider>();
    l1::log_info_collector log_info_collector(
      &_metastore, std::move(cfg_provider), std::move(offset_provider));

    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);

    // Seed the metastore with two small objects for this partition. With the
    // default config (max_object_size=80MiB, threshold=0.5 =>
    // min_acceptable=40MiB), each object is undersized. The leveling range
    // builder only emits a range when it sees a run of *two or more*
    // consecutive undersized extents (singletons can't reduce extent count),
    // so we need at least two objects to produce a non-empty range.
    model::offset o{0};
    {
        auto batches = model::test::make_random_batches(o, 10).get();
        o = model::next_offset(batches.back().last_offset());
        std::vector<tidp_batches_t> bs;
        bs.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(bs)).get();
    }

    {
        auto batches = model::test::make_random_batches(o, 10).get();
        std::vector<tidp_batches_t> bs;
        bs.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(bs)).get();
    }

    l1::log_set_t logs_set;
    auto [it, inserted] = logs_set.insert(log_ptr);
    ASSERT_TRUE(inserted);
    l1::log_list_t logs_list;
    logs_list.push_back(*log_ptr);
    l1::leveling_extent_reclamation_policy policy{
      config::mock_binding<size_t>(size_t{1024} * 1024)};
    l1::leveling_queue queue(policy.get_comparator());

    log_info_collector.collect_leveling_info(logs_set, logs_list, queue).get();

    ASSERT_TRUE(log_ptr->leveling.info_and_ts.has_value());
    // info.ranges is cleared after queueing; only the timestamp cookie is
    // retained for the next tick.
    ASSERT_TRUE(log_ptr->leveling.info_and_ts->info.ranges.empty());
    ASSERT_GT(queue.size(), 0u);
    ASSERT_EQ(queue.size(), log_ptr->leveling.outstanding_ranges);
    size_t total_size_bytes = 0;
    while (!queue.empty()) {
        total_size_bytes += queue.top()->range.size_bytes;
        queue.pop();
    }
    ASSERT_GT(total_size_bytes, 0u);
}
