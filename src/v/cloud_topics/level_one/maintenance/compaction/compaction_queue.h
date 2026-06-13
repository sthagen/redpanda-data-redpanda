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

#include "cloud_topics/level_one/maintenance/keyed_priority_queue.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

/// \brief A scheduling queue of compaction jobs, ordered best-first by a
/// caller-supplied `compaction_cmp_t`.
///
/// Keyed on `topic_id_partition`, so each CTP has at most one queued job:
/// re-queuing a CTP (e.g. after a fresh metastore sample) replaces its job in
/// place rather than appending a duplicate, and `clear(tidp)` evicts a CTP's
/// job when it is unmanaged. Analogous to `leveling_queue`, but with a single
/// job per CTP.
class compaction_queue {
public:
    explicit compaction_queue(compaction_cmp_t cmp)
      : _queue(std::move(cmp)) {}

    /// Enqueue `job`, or replace the existing job for its CTP in place. The
    /// owning CTP is taken from the job's meta.
    void push(compaction_job_ptr job) {
        const auto tidp = job->meta->tidp;
        _queue.upsert(tidp, std::move(job));
    }

    /// The highest-priority queued job. Precondition: non-empty.
    const compaction_job_ptr& top() const { return _queue.top().second; }

    /// Remove the highest-priority queued job. Precondition: non-empty.
    void pop() {
        auto tidp = _queue.top().first;
        _queue.erase(tidp);
    }

    /// Drop the queued job for `tidp`, if any (e.g. when a CTP is unmanaged).
    /// No-op if the CTP has nothing queued.
    void clear(const model::topic_id_partition& tidp) {
        if (_queue.contains(tidp)) {
            _queue.erase(tidp);
        }
    }

    /// Clear all entries in the queue.
    void clear_all() { _queue.clear(); }

    bool empty() const { return _queue.empty(); }
    size_t size() const { return _queue.size(); }
    bool contains(const model::topic_id_partition& tidp) const {
        return _queue.contains(tidp);
    }

private:
    keyed_priority_queue<
      model::topic_id_partition,
      compaction_job_ptr,
      compaction_cmp_t>
      _queue;
};

} // namespace cloud_topics::l1
