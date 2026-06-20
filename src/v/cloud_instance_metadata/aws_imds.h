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
#include <boost/beast/http/status.hpp>

#include <array>
#include <chrono>
#include <optional>
#include <string_view>

/// Request shaping for the AWS EC2 Instance Metadata Service (IMDS): endpoint
/// constants and request construction shared by every IMDS consumer
/// (cloud_roles credential refresh, instance-type detection). Consumers keep
/// their own transport, timeout, and error-handling policies; only the
/// protocol surface lives here.
/// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
namespace cloud_instance_metadata::aws_imds {

/// Link-local address of the IMDS endpoint, reachable from inside any EC2
/// instance.
constexpr std::string_view host = "169.254.169.254";
constexpr uint16_t port = 80;

/// IMDSv2 session token endpoint (PUT) and header names.
constexpr std::string_view token_path = "/latest/api/token";
constexpr std::string_view token_header = "X-aws-ec2-metadata-token";
constexpr std::string_view token_ttl_header
  = "X-aws-ec2-metadata-token-ttl-seconds";

/// Metadata path for this instance's type (e.g. "m6id.4xlarge").
constexpr std::string_view instance_type_path
  = "/latest/meta-data/instance-type";

/// Statuses of a token request on which AWS SDKs treat IMDSv2 as unavailable
/// and fall back to IMDSv1 (requests without a session token).
constexpr auto v1_fallback_statuses = std::to_array(
  {boost::beast::http::status::not_found,
   boost::beast::http::status::forbidden,
   boost::beast::http::status::method_not_allowed});

/// PUT request obtaining an IMDSv2 session token with the given TTL.
/// \p host is a parameter (not the constant above) because consumers may
/// target a test server or a configured endpoint override.
boost::beast::http::request_header<>
token_request(std::string_view host, std::chrono::seconds token_ttl);

/// GET request for a metadata path, carrying the IMDSv2 session token when one
/// is supplied (IMDSv1 fallback sends none).
boost::beast::http::request_header<> get(
  std::string_view host,
  std::string_view path,
  std::optional<std::string_view> token = std::nullopt);

} // namespace cloud_instance_metadata::aws_imds
