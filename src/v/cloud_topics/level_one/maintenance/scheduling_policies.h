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

#include "base/vassert.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "config/property.h"
#include "model/fundamental.h"

#include <iterator>

namespace cloud_topics::l1 {

class scheduling_policy {
public:
    scheduling_policy() = default;
    scheduling_policy(const scheduling_policy&) = delete;
    scheduling_policy& operator=(const scheduling_policy&) = delete;
    scheduling_policy(scheduling_policy&& other) noexcept = default;
    scheduling_policy& operator=(scheduling_policy&&) noexcept = default;
    virtual ~scheduling_policy() = default;

    virtual compaction_cmp_t get_comparator() const noexcept = 0;
};

// Compacts partitions from highest dirty ratio (the ratio of unclean bytes in
// the log to the total log size) to lowest.
class dirty_ratio_scheduling_policy : public scheduling_policy {
public:
    compaction_cmp_t get_comparator() const noexcept final;

private:
    struct sort_policy {
        static bool operator()(
          const compaction_job_ptr& a, const compaction_job_ptr& b) noexcept {
            return a->info_and_ts.info.dirty_ratio
                   < b->info_and_ts.info.dirty_ratio;
        }
    };
};

// Compacts partitions from highest compaction lag (the oldest timestamp of
// the first uncompacted record) to lowest.
class compaction_lag_scheduling_policy : public scheduling_policy {
public:
    compaction_cmp_t get_comparator() const noexcept final;

private:
    struct sort_policy {
        static bool operator()(
          const compaction_job_ptr& a, const compaction_job_ptr& b) noexcept {
            return a->info_and_ts.info.earliest_dirty_ts
                   > b->info_and_ts.info.earliest_dirty_ts;
        }
    };
};

std::unique_ptr<scheduling_policy> make_default_scheduling_policy();

// Orders leveling jobs by expected extent-count reduction, highest first.
// For each range, the expected number of output objects is approximated as
// `ceil(size_bytes / target_size_per_object)`, and the reclaim count is
// `extent_count - expected_outputs`.
class leveling_extent_reclamation_policy {
public:
    explicit leveling_extent_reclamation_policy(
      config::binding<size_t> target_size_per_object)
      : _target_size_per_object(std::move(target_size_per_object)) {}

    leveling_cmp_t get_comparator() const noexcept;

private:
    struct sort_policy {
        bool operator()(
          const leveling_job_ptr& a, const leveling_job_ptr& b) const noexcept {
            return expected_reclaim(a->range) < expected_reclaim(b->range);
        }

        size_t expected_reclaim(const levelable_range& r) const noexcept {
            const size_t target = target_size_per_object();
            vassert(
              target > 0,
              "leveling_extent_reclamation_policy requires a positive "
              "target_size_per_object");
            const size_t expected_outputs = std::max<size_t>(
              1, (r.size_bytes + target - 1) / target);
            return r.extent_count > expected_outputs
                     ? r.extent_count - expected_outputs
                     : 0;
        }

        config::binding<size_t> target_size_per_object;
    };

    config::binding<size_t> _target_size_per_object;
};

} // namespace cloud_topics::l1
