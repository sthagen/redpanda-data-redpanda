/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

// A wrapper to provide easy mocking and break dependency on a multitude of
// cluster objects within the `log_info_collector`.
class topic_cfg_provider {
public:
    virtual ~topic_cfg_provider() noexcept = default;

    virtual std::optional<
      std::reference_wrapper<const cluster::topic_configuration>>
      get_topic_cfg(model::topic_namespace_view) const = 0;
};

// Default topic_cfg_provider, which is the `cluster::metadata_cache`.
class topic_cfg_provider_impl : public topic_cfg_provider {
public:
    topic_cfg_provider_impl(cluster::metadata_cache*);

    std::optional<std::reference_wrapper<const cluster::topic_configuration>>
      get_topic_cfg(model::topic_namespace_view) const final;

private:
    cluster::metadata_cache* _metadata_cache;
};

// Provides the maximum offset which is compactible for a given ntp.
class max_compactible_offset_provider {
public:
    virtual ~max_compactible_offset_provider() noexcept = default;

    // Fills the provided map with max compactible offsets for the given NTPs.
    // NTPs that cannot be looked up (e.g. partition not found) will not have
    // an entry added to the map.
    virtual ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const = 0;
};

// Default max_compactible_offset_provider, which uses the `shard_table` and
// `partition_manager` to access a partition's `lowest_pinned_data_offset()`
// through its `stm_hookset()`. Batches cross-shard calls by grouping NTPs
// by their owning shard.
class max_compactible_offset_provider_impl
  : public max_compactible_offset_provider {
public:
    max_compactible_offset_provider_impl(
      ss::sharded<cluster::shard_table>*,
      ss::sharded<cluster::partition_manager>*);

    ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const final;

private:
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::partition_manager>* _partition_manager;
};

// Responsible for issuing `get_compaction_info()` requests to the `metastore`
// when attempting to schedule a round of compactions.
class log_info_collector {
public:
    log_info_collector(
      metastore*,
      std::unique_ptr<topic_cfg_provider>,
      std::unique_ptr<max_compactible_offset_provider>);

    // Populates `compaction.info_and_ts` within `log_compaction_meta`s from
    // the provided `log_list_t` by collecting each log's compaction info from
    // the metastore. It is not guaranteed that every log present in
    // `log_list_t` will have its `compaction.info_and_ts` set e.g. due to
    // concurrent removal or metastore errors. If a log already has
    // `compaction.info_and_ts` set, it will not be collected again until an
    // interval has elapsed and the current `compaction.info_and_ts` is
    // determined stale. Additionally, logs that have an inflight compaction in
    // process do not need to be collected. Logs that have their information
    // collected and deemed eligible for compaction will also have their
    // `lw_shared_ptr` copied into the `log_compaction_queue` for future
    // compaction.
    ss::future<> collect_compaction_info(
      log_set_t&, log_list_t&, log_compaction_queue&) const;

    // Populates `leveling.info_and_ts` within `log_compaction_meta`s from the
    // provided `log_list_t` by collecting each log's leveling info from the
    // metastore. Logs are skipped if `leveling.info_and_ts` is still fresh.
    // For freshly-sampled logs, per-range `leveling_job`s are pushed into the
    // provided `leveling_queue` and `leveling.outstanding_ranges` is bumped
    // accordingly. The transient `info.ranges` is cleared after queueing
    // while the `collected_at` timestamp is retained so the next tick
    // respects the sampling interval.
    ss::future<>
    collect_leveling_info(log_set_t&, log_list_t&, leveling_queue&) const;

private:
    // Returns a container of `compaction_info_spec` to sample the metastore
    // with based on the input `log_list_t`.
    chunked_vector<metastore::compaction_info_spec>
    build_compaction_specs(log_list_t&, size_t, model::timestamp) const;

    // Returns a container of `leveling_info_spec` to sample the metastore
    // with based on the input `log_list_t`. Skips logs whose cached
    // `leveling.info_and_ts` is still fresh.
    chunked_vector<metastore::leveling_info_spec>
    build_leveling_specs(log_list_t&, model::timestamp) const;

    // Sets compaction info state within the input logs per the
    // `compaction_info_map` collected from the metastore and pushes logs
    // eligible for compaction to the provided `log_compaction_queue`.
    void populate_logs_with_compaction_info(
      metastore::compaction_info_map&,
      log_set_t&,
      log_list_t&,
      log_compaction_queue&,
      const chunked_hash_map<model::ntp, kafka::offset>&,
      model::timestamp) const;

    // Sets leveling info state within the input logs per the
    // `leveling_info_map` collected from the metastore. For each freshly-
    // sampled log with non-empty ranges, pushes per-range `leveling_job`s
    // into the provided `leveling_queue` and bumps the meta's
    // `leveling.outstanding_ranges`. Clears `info.ranges` after queueing
    // while retaining `collected_at` as a rate-limit cookie for the next
    // tick.
    void populate_logs_with_leveling_info(
      metastore::leveling_info_map&,
      log_set_t&,
      leveling_queue&,
      model::timestamp) const;

    // Owned by `app`.
    metastore* _metastore;

    std::unique_ptr<topic_cfg_provider> _topic_metadata_provider;
    std::unique_ptr<max_compactible_offset_provider>
      _max_compactible_offset_provider;
};

log_info_collector make_default_log_info_collector(
  metastore*,
  cluster::metadata_cache*,
  ss::sharded<cluster::shard_table>*,
  ss::sharded<cluster::partition_manager>*);

} // namespace cloud_topics::l1
