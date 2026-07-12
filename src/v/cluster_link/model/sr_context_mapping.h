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
#include "pandaproxy/schema_registry/types.h"

namespace cluster_link::model {

/// Returns true if the API-sync source filter selects \p source_context.
///
/// An empty filter (no contexts and no subjects) selects every source context.
/// This is the single source of truth for filter membership, shared by the
/// runtime client-write blocker (cluster/cluster_link/frontend.cc) and the
/// creation-time Schema Registry preflight check
/// (cluster_link/sr_preflight_checker.cc) so the two cannot drift.
bool filter_selects_source_context(
  const schema_registry_sync_config::source_filter& filter,
  const pandaproxy::schema_registry::context& source_context);

} // namespace cluster_link::model
