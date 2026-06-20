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

/// Request shaping for the Azure Instance Metadata Service (IMDS): endpoint
/// constants and request construction. Consumers keep their own transport,
/// timeout, and error-handling policies; only the protocol surface lives here.
/// https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service
namespace cloud_instance_metadata::azure_imds {

/// Link-local address of the IMDS endpoint, reachable from inside any Azure
/// VM.
constexpr std::string_view host = "169.254.169.254";
constexpr uint16_t port = 80;

/// Header IMDS requires on every request, as proof the request was made
/// deliberately rather than by an unaware proxy.
constexpr std::string_view metadata_header = "Metadata";
constexpr std::string_view metadata_value = "true";

/// GET request for an IMDS path with the required Metadata header. \p path
/// carries the api-version query parameter (IMDS requires one on every
/// request) and any others. No Host header is set, preserving established
/// consumer behavior; IMDS does not require one.
boost::beast::http::request_header<> get(std::string_view path);

} // namespace cloud_instance_metadata::azure_imds
