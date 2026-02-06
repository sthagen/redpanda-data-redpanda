// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_storage/topic_path_provider.h"

#include "cloud_storage/topic_path_utils.h"

#include <utility>

namespace cloud_storage {

topic_path_provider::topic_path_provider(
  std::optional<remote_label> label,
  std::optional<model::topic_namespace> topic_namespace_override)
  : label_(label)
  , topic_namespace_override_(std::move(topic_namespace_override)) {}

topic_path_provider topic_path_provider::copy() const {
    topic_path_provider ret(label_, topic_namespace_override_);
    return ret;
}

ss::sstring topic_path_provider::topic_manifest_prefix(
  const model::topic_namespace& topic) const {
    const auto& tp_ns = topic_namespace_override_.value_or(topic);
    if (label_.has_value()) {
        return labeled_topic_manifest_prefix(*label_, tp_ns);
    }
    return prefixed_topic_manifest_prefix(tp_ns);
}

ss::sstring topic_path_provider::topic_manifest_path(
  const model::topic_namespace& topic, model::initial_revision_id rev) const {
    const auto& tp_ns = topic_namespace_override_.value_or(topic);
    if (label_.has_value()) {
        return labeled_topic_manifest_path(*label_, tp_ns, rev);
    }
    return prefixed_topic_manifest_bin_path(tp_ns);
}

std::optional<ss::sstring> topic_path_provider::topic_manifest_path_json(
  const model::topic_namespace& topic) const {
    if (label_.has_value()) {
        return std::nullopt;
    }
    const auto& tp_ns = topic_namespace_override_.value_or(topic);
    return prefixed_topic_manifest_json_path(tp_ns);
}

ss::sstring topic_path_provider::topic_lifecycle_marker_path(
  const model::topic_namespace& topic, model::initial_revision_id rev) const {
    const auto& tp_ns = topic_namespace_override_.value_or(topic);
    if (label_.has_value()) {
        return labeled_topic_lifecycle_marker_path(*label_, tp_ns, rev);
    }
    return prefixed_topic_lifecycle_marker_path(tp_ns, rev);
}

} // namespace cloud_storage
