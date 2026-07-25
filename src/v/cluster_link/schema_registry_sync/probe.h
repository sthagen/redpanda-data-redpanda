/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster_link/model/types.h"
#include "metrics/metrics.h"

#include <seastar/util/noncopyable_function.hh>

#include <optional>

namespace cluster_link::schema_registry_sync {

/// Exposes a Schema Registry shadowing task's totals_since_task_start
/// counters on the internal and public prometheus endpoints.
///
/// The owning task registers the series when it starts leading
/// `_schemas/0` and keeps them registered while paused (the totals
/// survive a pause). They are removed when stop() completes: the task
/// state flips to `stopped` before the runner drains, so a scrape
/// during that window still sees the series. Stopping resets the
/// totals, so a stale series cannot linger after leadership moves
/// away; a transfer may briefly export the series on zero or two nodes.
class probe {
public:
    using totals_fetcher
      = ss::noncopyable_function<model::schema_registry_sync_summary()>;

    probe() = default;
    probe(const probe&) = delete;
    probe& operator=(const probe&) = delete;
    probe(probe&&) = delete;
    probe& operator=(probe&&) = delete;
    ~probe() = default;

    /// Registers the counter series for `link_name`, fetching the current
    /// totals through `get_totals` on every scrape. Idempotent: resuming a
    /// paused task invokes it again.
    void setup(const model::name_t& link_name, totals_fetcher get_totals);

    /// Removes the registered series.
    void clear() { _metrics.reset(); }

private:
    totals_fetcher _get_totals;
    std::optional<metrics::all_metrics_groups> _metrics;
};

} // namespace cluster_link::schema_registry_sync
