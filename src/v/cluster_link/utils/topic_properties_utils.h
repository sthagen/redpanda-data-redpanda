/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "features/fwd.h"
#include "kafka/server/handlers/configs/config_utils.h"

namespace cluster_link::utils {

bool maybe_append_update(
  cluster::topic_properties_update& update,
  const ss::sstring& config_name,
  const ss::sstring& config_value,
  const cluster::topic_configuration& topic_config);

/// Combine the source topic's redpanda.storage.mode and its read-only
/// redpanda.storage.mode.impl companion into a storage-mode update. The
/// impl value is exact and wins; the mode value alone is the fallback for
/// sources that predate the impl property (where 'tiered' meant the
/// classic tiered storage). Updates that are not permitted storage-mode
/// transitions are rejected with a validation error rather than applied,
/// and so is a move to the tiered_v2 impl while the tiered_cloud_topics
/// feature is not active (the cluster is not fully upgraded to v26.2) --
/// this internal path must enforce the same gate as the kafka handlers.
bool maybe_append_storage_mode_update(
  cluster::topic_properties_update& update,
  const std::optional<ss::sstring>& mode_value,
  const std::optional<ss::sstring>& impl_value,
  const cluster::topic_configuration& topic_config,
  const features::feature_table& feature_table);

} // namespace cluster_link::utils
