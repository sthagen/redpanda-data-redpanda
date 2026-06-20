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

#include "base/seastarx.h"
#include "metrics/instance_info.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <optional>

namespace instance_info {

/// A detected cloud instance type together with its resolved info.
struct detected_instance {
    cloud_provider provider;
    ss::sstring instance_type;
    /// Capacity figures for this instance type, or nullopt when the type was
    /// detected (provider and name are known) but is absent from our generated
    /// table, so its capacity is unknown.
    std::optional<capacity_info> capacity;
};

/// The environment variable that, when set, overrides instance-type detection.
/// Mainly for testing and for hosts where the metadata service is unavailable
/// (e.g. bare metal). The value is an instance-type name resolved across all
/// providers (e.g. "m6id.4xlarge").
constexpr const char* instance_type_env_var = "REDPANDA_INSTANCE_TYPE";

/// Detect this node's cloud instance type and resolve its capacity.
///
/// Checks the REDPANDA_INSTANCE_TYPE override first; otherwise queries the EC2
/// IMDSv2 metadata service. When an instance is detected, returns a
/// detected_instance carrying its identity; its capacity is left unset when the
/// type is absent from the generated table (the identity is still known).
/// Returns nullopt only when no instance is detected: detection fails, this is
/// not a recognized cloud instance, or the override value is not a known
/// instance type. Never throws: any error is logged and reported as nullopt.
ss::future<std::optional<detected_instance>>
detect_instance(ss::abort_source& as);

} // namespace instance_info
