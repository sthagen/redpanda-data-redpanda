// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cloud_storage/remote_label.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace cloud_storage {

/// Provides path computation for topic-level cloud storage objects.
/// This is a subset of remote_path_provider's functionality that can be used
/// independently without depending on partition_manifest and related types.
class topic_path_provider {
public:
    topic_path_provider(const topic_path_provider&) = delete;
    topic_path_provider& operator=(const topic_path_provider&) = delete;
    topic_path_provider& operator=(topic_path_provider&&) = delete;
    ~topic_path_provider() = default;

    explicit topic_path_provider(
      std::optional<remote_label> label,
      std::optional<model::topic_namespace> topic_namespace_override);

    topic_path_provider copy() const;
    topic_path_provider(topic_path_provider&&) = default;

    // Prefix of the topic manifest path. This can be used to filter objects to
    // find topic manifests.
    ss::sstring
    topic_manifest_prefix(const model::topic_namespace& topic) const;

    // Topic manifest path.
    ss::sstring topic_manifest_path(
      const model::topic_namespace& topic, model::initial_revision_id) const;
    std::optional<ss::sstring>
    topic_manifest_path_json(const model::topic_namespace& topic) const;

    // Topic lifecycle marker path.
    ss::sstring topic_lifecycle_marker_path(
      const model::topic_namespace& topic,
      model::initial_revision_id rev) const;

    const std::optional<remote_label>& label() const { return label_; }
    const std::optional<model::topic_namespace>&
    topic_namespace_override() const {
        return topic_namespace_override_;
    }

protected:
    std::optional<remote_label> label_;
    std::optional<model::topic_namespace> topic_namespace_override_;
};

} // namespace cloud_storage
