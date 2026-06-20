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

// Implementation detail of the instance-type detector, split out so the public
// interface (instance_type_detector.h) doesn't pull in the http client. Used by
// the detector itself and by its unit tests.

#include "base/seastarx.h"
#include "http/client.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <optional>

namespace instance_info {

/// Query the EC2 IMDSv2 metadata service for the instance-type string (e.g.
/// "m6id.4xlarge"). Performs the PUT-token then GET-metadata handshake.
/// Takes an http::abstract_client so it can be exercised against a mock;
/// returns nullopt on any failure.
ss::future<std::optional<ss::sstring>> query_ec2_instance_type(
  http::abstract_client& client, ss::lowres_clock::duration timeout);

} // namespace instance_info
