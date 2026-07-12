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

#include "container/chunked_vector.h"
#include "security/role.h"
#include "security/types.h"

namespace cluster_link {

/// The set of role mutations needed to make the shadow cluster's in-scope
/// roles match the source's in-scope roles. Inputs to reconcile_roles are
/// already filtered to the configured selection, so this struct carries no
/// policy.
struct role_changes {
    chunked_vector<security::role_with_members> to_create;
    chunked_vector<security::role_with_members> to_update;
    chunked_vector<security::role_name> to_delete;
};

/// Compute the full-mirror diff between two already-filtered role sets:
///   to_create   = source \ shadow (by name)
///   to_update   = names in both whose membership differs
///   to_delete   = shadow \ source (by name)
role_changes reconcile_roles(
  chunked_vector<security::role_with_members> source_selected,
  chunked_vector<security::role_with_members> shadow_selected);

} // namespace cluster_link
