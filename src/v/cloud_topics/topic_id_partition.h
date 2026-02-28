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

#include "cloud_topics/state_accessors.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <optional>

namespace cloud_topics {

/// Thrown when the topic config lookup fails, which can occur when the topic
/// config has been removed from the topic_table but a partition operation is
/// still in flight.
struct topic_config_not_found_exception final : ss::abort_requested_exception {
    explicit topic_config_not_found_exception(const model::ntp& ntp)
      : _msg(
          fmt::format(
            "topic config not found for {} (topic likely deleted)", ntp)) {}
    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

/// Look up the topic_id_partition for a cloud-topics partition.
/// Returns nullopt when the topic config has been removed from the
/// topic_table (e.g. during deletion) but the partition has not yet
/// been shut down.
inline std::optional<model::topic_id_partition>
get_topic_id_partition(const ss::lw_shared_ptr<cluster::partition>& partition) {
    const auto& ntp = partition->ntp();
    auto ct_state = partition->get_cloud_topics_state();
    auto metadata_cache = ct_state->local().get_metadata_cache();
    auto topic_cfg = metadata_cache->get_topic_cfg(
      model::topic_namespace_view(ntp));
    if (!topic_cfg || !topic_cfg->tp_id) {
        return std::nullopt;
    }
    return model::topic_id_partition{*topic_cfg->tp_id, ntp.tp.partition};
}

} // namespace cloud_topics
