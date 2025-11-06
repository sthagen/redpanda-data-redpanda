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

#include "cluster/fwd.h"
#include "features/feature_table.h"
#include "model/fundamental.h"

namespace admin::utils {

void check_license(const features::feature_table& ft);

/**
 * @brief Used to indicate if a request should be redirected to a partition
 * leader
 *
 * @return Leader node id for the NTP or std::nullopt already on leader node
 * @throws serde::pb::rpc::unavailable_exception if no leader exists
 */
std::optional<model::node_id> redirect_to_leader(
  const cluster::metadata_cache& md_cache,
  const model::ntp& ntp,
  model::node_id self);

} // namespace admin::utils
