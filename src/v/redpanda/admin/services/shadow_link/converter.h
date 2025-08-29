/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#pragma once

#include "cluster_link/model/types.h"
#include "proto/redpanda/core/admin/v2/shadow_link.proto.h"

namespace admin {

/// \brief Converts a create cluster link request into a cluster link metadata
/// object
///
/// \throws serde::pb::rpc::invalid_argument_exception if the request contains
/// invalid data
cluster_link::model::metadata
convert_create_to_metadata(proto::admin::create_shadow_link_request req);

/// \brief Converts a cluster link metadata object into a shadow link resource
proto::admin::shadow_link
metadata_to_shadow_link(cluster_link::model::metadata md);
} // namespace admin
