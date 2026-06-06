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

#include "base/format_to.h"
#include "cloud_topics/level_one/metastore/leveling_range_builder.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_map.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace cloud_topics::l1 {

// Contains compaction information collected from the metastore and the time at
// which it was obtained.
struct compaction_info_and_timestamp {
    metastore::compaction_info_response info;
    model::timestamp collected_at;
    kafka::offset max_compactible_offset;
};

// Per-CTP state for the compaction maintenance subsystem.
struct log_compaction_state {
    // If set, the worker shard on which this CTP is currently undergoing an
    // inflight compaction. A CTP's scheduling state is derived rather than
    // stored: it is `inflight` iff this has a value, `queued` iff the
    // scheduler's `compaction_queue` holds a job for it, and `idle` otherwise.
    // Mutated only on `worker_manager_shard`.
    std::optional<ss::shard_id> inflight_shard{std::nullopt};
};

// Per-CTP state for the leveling maintenance subsystem.
struct log_leveling_state {
    // Refcount of inflight leveling ranges per worker shard for this CTP.
    chunked_hash_map<ss::shard_id, size_t> inflight_shards;

    // Leveling ranges for this CTP that have been dequeued for leveling and are
    // inflight (mapped to nullopt) or have since committed (mapped to their
    // completion timestamp), keyed by offset range. The collector consults this
    // to avoid re-queueing a range that *overlaps* one already inflight: a
    // range stays "undersized" in the metastore until its rewrite commits, and
    // that commit is not visible to a sample taken before it, so without this
    // we would re-queue an overlapping replacement every tick. A committed
    // entry is evicted once its timestamp predates a collection's snapshot, at
    // which point the metastore is guaranteed to reflect the commit. Mutated
    // only on `worker_manager_shard`.
    offset_interval_map<std::optional<model::timestamp>> inflight_ranges;
};

struct log_compaction_meta {
    log_compaction_meta(model::topic_id_partition tidp, model::ntp ntp)
      : tidp(std::move(tidp))
      , ntp(std::move(ntp)) {}

    model::topic_id_partition tidp;
    model::ntp ntp;
    intrusive_list_hook link;
    // If `true`, we have been able to sample info from the `metastore` for this
    // CTP previously.
    bool has_seen_reconciled_data{false};

    log_compaction_state compaction;
    log_leveling_state leveling;
};

using log_compaction_meta_ptr = ss::lw_shared_ptr<log_compaction_meta>;

struct log_compaction_meta_hash {
    using is_transparent = void;

    size_t
    operator()(const cloud_topics::l1::log_compaction_meta_ptr& m) const {
        return absl::Hash<model::topic_id_partition>{}(m->tidp);
    }

    size_t operator()(const model::topic_id_partition& tidp) const {
        return absl::Hash<model::topic_id_partition>{}(tidp);
    }
};

struct log_compaction_meta_eq {
    using is_transparent = void;

    bool operator()(
      const cloud_topics::l1::log_compaction_meta_ptr& lhs,
      const cloud_topics::l1::log_compaction_meta_ptr& rhs) const {
        return lhs->tidp == rhs->tidp;
    }

    bool operator()(
      const cloud_topics::l1::log_compaction_meta_ptr& lhs,
      const model::topic_id_partition& rhs) const noexcept {
        return lhs->tidp == rhs;
    }

    bool operator()(
      const model::topic_id_partition& lhs,
      const cloud_topics::l1::log_compaction_meta_ptr& rhs) const {
        return lhs == rhs->tidp;
    }
};

using log_set_t = chunked_hash_set<
  log_compaction_meta_ptr,
  log_compaction_meta_hash,
  log_compaction_meta_eq>;

using log_list_t
  = intrusive_list<log_compaction_meta, &log_compaction_meta::link>;

// A compaction of a single CTP, scheduled as a job. Holds the owning CTP's
// meta (for identity and inflight bookkeeping) alongside the metastore sample
// the compaction will run against.
struct compaction_job {
    compaction_job(
      log_compaction_meta_ptr meta, compaction_info_and_timestamp info_and_ts)
      : meta(std::move(meta))
      , info_and_ts(std::move(info_and_ts)) {}

    log_compaction_meta_ptr meta;
    compaction_info_and_timestamp info_and_ts;
};

using compaction_job_ptr = ss::lw_shared_ptr<compaction_job>;
using foreign_compaction_job_ptr = ss::foreign_ptr<compaction_job_ptr>;

using compaction_cmp_t
  = std::function<bool(const compaction_job_ptr&, const compaction_job_ptr&)>;

// A single levelable range scheduled as an independent job. Holds a
// back-link to the per-CTP meta so the worker_manager can find inflight
// ranges by tidp for preemption, and so per-log probes can be attributed.
struct leveling_job {
    leveling_job(
      log_compaction_meta_ptr meta,
      levelable_range range,
      metastore::compaction_epoch epoch)
      : meta(std::move(meta))
      , range(range)
      , epoch(epoch) {}

    log_compaction_meta_ptr meta;
    levelable_range range;
    metastore::compaction_epoch epoch;
};

using leveling_job_ptr = ss::lw_shared_ptr<leveling_job>;
using foreign_leveling_job_ptr = ss::foreign_ptr<leveling_job_ptr>;

using leveling_cmp_t
  = std::function<bool(const leveling_job_ptr&, const leveling_job_ptr&)>;

enum class compaction_job_state {
    // No compaction job is currently inflight.
    idle,
    // A compaction job is currently inflight.
    running,
    // A graceful stop has been requested of an inflight compaction job.
    // The user should try to commit as much useful data as possible while still
    // shutting down in a prompt manner.
    soft_stop,
    // A forceful stop has been requested of an inflight compaction job.
    // The user should abandon any work and shutdown immediately.
    hard_stop
};

inline fmt::iterator format_to(compaction_job_state s, fmt::iterator out) {
    switch (s) {
    case compaction_job_state::idle:
        return fmt::format_to(out, "idle");
    case compaction_job_state::running:
        return fmt::format_to(out, "running");
    case compaction_job_state::soft_stop:
        return fmt::format_to(out, "soft_stop");
    case compaction_job_state::hard_stop:
        return fmt::format_to(out, "hard_stop");
    }
}

} // namespace cloud_topics::l1
