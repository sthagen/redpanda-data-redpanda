/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/vassert.h"
#include "cloud_topics/level_one/maintenance/keyed_priority_queue.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <queue>

namespace cloud_topics::l1 {

/// \brief A two-level scheduling queue for leveling jobs that performs a k-way
/// merge across partitions.
///
/// Each partition (CTP) owns an inner priority queue of its `leveling_job`s,
/// ordered best-first by a caller-supplied `leveling_cmp_t`. An addressable
/// outer queue (`keyed_priority_queue`) tracks each partition's *current best*
/// job, so `pop()` returns the globally best job.
class leveling_queue {
public:
    explicit leveling_queue(leveling_cmp_t cmp)
      : _cmp(cmp)
      , _heads(std::move(cmp)) {}

    /// Enqueue `job`. The owning partition is taken from the job's metadata.
    void push(leveling_job_ptr job) {
        const auto& tidp = job->meta->tidp;
        // Inner queues share the comparator; a default-constructed
        // leveling_cmp_t would be an empty std::function, so construct each
        // queue with `_cmp`.
        auto& queue = _queues.try_emplace(tidp, _cmp).first->second;
        // Only the partition's best job is exposed to the outer queue, so we
        // only need to (re)publish its head when this job becomes the best.
        const bool new_best = queue.empty() || _cmp(queue.top(), job);
        queue.push(std::move(job));
        if (new_best) {
            _heads.upsert(tidp, queue.top());
        }
        ++_size;
    }

    /// The globally best job. Precondition: non-empty.
    const leveling_job_ptr& top() const {
        vassert(!empty(), "leveling_queue::top on an empty queue");
        return _heads.top().second;
    }

    /// Remove the globally best job. No-op if empty.
    void pop() {
        if (_heads.empty()) {
            return;
        }
        const auto tidp = _heads.top().first;
        auto queue_it = _queues.find(tidp);
        vassert(
          queue_it != _queues.end() && !queue_it->second.empty(),
          "leveling_queue head {} has no backing job",
          tidp);
        auto& queue = queue_it->second;
        queue.pop();
        if (queue.empty()) {
            _queues.erase(queue_it);
            _heads.erase(tidp);
        } else {
            // Advance this queue: publish its new best to the outer queue.
            _heads.upsert(tidp, queue.top());
        }
        --_size;
    }

    /// Drop all queued jobs for `tidp` (e.g. before rebuilding the partition
    /// from a fresh metastore sample). No-op if the partition has none queued.
    void clear(const model::topic_id_partition& tidp) {
        auto queue_it = _queues.find(tidp);
        if (queue_it == _queues.end()) {
            return;
        }
        _size -= queue_it->second.size();
        _queues.erase(queue_it);
        _heads.erase(tidp);
    }

    /// Clear all entries across both levels of the queue.
    void clear_all() {
        _queues.clear();
        _heads.clear();
        _size = 0;
    }

    /// Total number of queued jobs across all partitions.
    size_t size() const { return _size; }
    bool empty() const { return _size == 0; }
    /// Number of partitions with at least one queued job.
    size_t partition_count() const { return _heads.size(); }

private:
    using inner_pq = std::priority_queue<
      leveling_job_ptr,
      chunked_vector<leveling_job_ptr>,
      leveling_cmp_t>;

    leveling_cmp_t _cmp;
    chunked_hash_map<model::topic_id_partition, inner_pq> _queues;
    keyed_priority_queue<
      model::topic_id_partition,
      leveling_job_ptr,
      leveling_cmp_t>
      _heads;
    size_t _size{0};
};

} // namespace cloud_topics::l1
