/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vassert.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/level_one/maintenance/log_info_collector.h"
#include "cloud_topics/level_one/maintenance/scheduler.h"
#include "cloud_topics/level_one/maintenance/scheduling_policies.h"
#include "cloud_topics/level_one/maintenance/worker_manager.h"
#include "cluster/topic_configuration.h"
#include "cluster/topic_properties.h"
#include "container/chunked_hash_map.h"

class fake_topic_metadata_provider : public l1::topic_cfg_provider {
public:
    std::optional<std::reference_wrapper<const cluster::topic_configuration>>
    get_topic_cfg(model::topic_namespace_view tp) const final {
        if (!_topic_metadata.contains(tp)) {
            cluster::topic_configuration cfg;
            cfg.properties.min_cleanable_dirty_ratio = tristate<double>{0.0};
            _topic_metadata.emplace(tp, cfg);
        }

        return _topic_metadata.at(tp);
    }

private:
    using underlying_t = chunked_hash_map<
      model::topic_namespace,
      cluster::topic_configuration,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    mutable underlying_t _topic_metadata;
};

class fake_offset_provider : public l1::max_compactible_offset_provider {
public:
    ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const final {
        co_return;
    }
};

class SchedulerTestFixture : public l1::l1_reader_fixture {
public:
    ss::future<> SetUpAsync() override { co_await start_scheduler(); }

    ss::future<> start_scheduler() {
        auto info_collector = l1::log_info_collector(
          &_metastore,
          std::make_unique<fake_topic_metadata_provider>(),
          std::make_unique<fake_offset_provider>());
        // not `std::make_unique` because private `compaction_scheduler` c-tor.
        scheduler = std::unique_ptr<l1::compaction_scheduler>(
          new l1::compaction_scheduler(std::move(info_collector)));
        co_await scheduler->_worker_manager._workers.start(
          &scheduler->_worker_manager,
          ss::sharded_parameter([this] { return &_io; }),
          ss::sharded_parameter([this] { return &_metastore; }),
          nullptr,
          ss::default_scheduling_group(),
          nullptr);
        co_await scheduler->_worker_manager._workers.invoke_on_all(
          &l1::compaction_worker::start);
        scheduler->start_bg_loop();
        co_return;
    }

    // Marks a managed CTP as queued for compaction and pushes it into the
    // scheduler's compaction queue, as the info collector would.
    void enqueue_for_compaction(const model::topic_id_partition& tidp) {
        auto it = scheduler->_logs.find(tidp);
        vassert(it != scheduler->_logs.end(), "CTP {} is not managed", tidp);
        scheduler->_compaction_queue.push(
          ss::make_lw_shared<l1::compaction_job>(
            *it, l1::compaction_info_and_timestamp{}));
    }

    bool compaction_queue_contains(const model::topic_id_partition& tidp) {
        return scheduler->_compaction_queue.contains(tidp);
    }

    size_t compaction_queue_size() {
        return scheduler->_compaction_queue.size();
    }

    ss::future<> pause_worker(ss::shard_id shard) {
        co_await scheduler->_worker_manager.pause_worker(shard);
    }

    ss::future<> resume_worker(ss::shard_id shard) {
        co_await scheduler->_worker_manager.resume_worker(shard);
    }

    ss::future<> TearDownAsync() override { co_await scheduler->stop(); }

protected:
    std::unique_ptr<l1::compaction_scheduler> scheduler{nullptr};
};
