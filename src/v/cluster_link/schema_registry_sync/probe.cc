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

#include "cluster_link/schema_registry_sync/probe.h"

#include "cluster_link/link_probe.h"

#include <seastar/core/metrics.hh>

namespace sm = ss::metrics;

namespace cluster_link::schema_registry_sync {

void probe::setup(const model::name_t& link_name, totals_fetcher get_totals) {
    if (_metrics.has_value()) {
        return;
    }

    const auto sl_name = link_probe::shadow_link_name(link_name);
    _get_totals = std::move(get_totals);

    _metrics.emplace().add_group(
      link_probe::shadow_link_group,
      {
        sm::make_counter(
          "schema_registry_subject_versions_changed",
          [this] { return _get_totals().subject_versions_changed; },
          sm::description(
            "Number of subject versions created, updated or deleted on the "
            "destination by Schema Registry shadowing since the task started"),
          {sl_name})
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "schema_registry_compatibility_configs_changed",
          [this] { return _get_totals().compatibility_configs_changed; },
          sm::description(
            "Number of compatibility configuration changes applied to the "
            "destination by Schema Registry shadowing since the task started"),
          {sl_name})
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "schema_registry_modes_changed",
          [this] { return _get_totals().modes_changed; },
          sm::description(
            "Number of mode changes applied to the destination by Schema "
            "Registry shadowing since the task started"),
          {sl_name})
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "schema_registry_unsupported_features_removed",
          [this] { return _get_totals().unsupported_features_removed; },
          sm::description(
            "Number of unsupported schema features removed from replicated "
            "schemas by Schema Registry shadowing since the task started"),
          {sl_name})
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "schema_registry_errors",
          [this] { return _get_totals().errors; },
          sm::description(
            "Number of errors observed by Schema Registry "
            "shadowing since the task started"),
          {sl_name})
          .aggregate({sm::shard_label}),
      });
}

} // namespace cluster_link::schema_registry_sync
