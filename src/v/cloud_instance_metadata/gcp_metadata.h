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

#include <boost/beast/http/message.hpp>

#include <string_view>

/// Request shaping for the GCE metadata server: endpoint constants and
/// request construction. Consumers keep their own transport, timeout, and
/// error-handling policies; only the protocol surface lives here.
/// https://cloud.google.com/compute/docs/metadata/overview
namespace cloud_instance_metadata::gcp_metadata {

/// Link-local address of the metadata server, reachable from inside any GCE
/// instance (also resolvable as metadata.google.internal).
constexpr std::string_view host = "169.254.169.254";
constexpr uint16_t port = 80;

/// Header the metadata server requires on every request, as proof the
/// request was made deliberately rather than by an unaware proxy.
constexpr std::string_view flavor_header = "Metadata-Flavor";
constexpr std::string_view flavor_value = "Google";

/// GET request for a metadata path with the required Metadata-Flavor header.
/// No Host header is set, preserving established consumer behavior; the
/// metadata server does not require one.
boost::beast::http::request_header<> get(std::string_view path);

} // namespace cloud_instance_metadata::gcp_metadata
