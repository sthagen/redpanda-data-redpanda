/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "aws_refresh_impl.h"

#include "cloud_instance_metadata/aws_imds.h"
#include "cloud_roles/logger.h"
#include "request_response_helpers.h"
#include "strings/utf8.h"

#include <algorithm>

namespace imds = cloud_instance_metadata::aws_imds;

namespace {

ss::sstring read_string_from_response(cloud_roles::api_response response) {
    vassert(
      std::holds_alternative<iobuf>(response),
      "response does not contain iobuf");
    auto str = std::get<iobuf>(response).linearize_to_string();
    validate_utf8(str);
    return str;
}

// TTL of the IMDSv2 session token requested for each fetch cycle.
constexpr auto imds_token_ttl = std::chrono::seconds{21600};

} // namespace

namespace cloud_roles {

struct ec2_response_schema {
    static constexpr std::string_view expiry = "Expiration";
    static constexpr std::string_view access_key_id = "AccessKeyId";
    static constexpr std::string_view secret_access_key = "SecretAccessKey";
    static constexpr std::string_view session_token = "Token";
};

aws_refresh_impl::aws_refresh_impl(
  net::unresolved_address address,
  aws_service_name service,
  aws_region_name region,
  ss::abort_source& as,
  retry_params retry_params,
  ss::sstring metrics_tag)
  : refresh_credentials::impl(
      std::move(address),
      std::move(region),
      as,
      retry_params,
      std::move(metrics_tag))
  , _service(std::move(service)) {}

bool aws_refresh_impl::is_fallback_required(const api_request_error& response) {
    return std::ranges::contains(imds::v1_fallback_statuses, response.status);
}

ss::future<api_response> aws_refresh_impl::fetch_credentials() {
    std::optional<ss::sstring> token = std::nullopt;
    if (likely(_fallback_engaged == fallback_engaged::no)) {
        // Although this token is valid for 6 hours, we always create a new one
        // for each fetch cycle. This removes the need to manage expiry of an
        // extra token. Since our credentials also live for multiple hours, we
        // do not need to make many extra calls to the instance metadata API,
        // except in cases where we need to retry on transient failures.
        auto token_response = co_await fetch_instance_metadata_token();
        if (std::holds_alternative<api_request_error>(token_response)) {
            const auto& error_response = std::get<api_request_error>(
              token_response);
            if (is_fallback_required(error_response)) {
                // If the request to get a token fails due to missing IMDSv2, we
                // fallback to v1 permanently for the lifetime of this
                // aws_refresh_impl, this is the behavior used by AWS SDKs.
                vlog(
                  clrl_log.warn,
                  "Failed to get IMDSv2 token, engaging fallback mode. "
                  "Response received for token request: {}",
                  error_response);
                _fallback_engaged = fallback_engaged::yes;
            } else {
                vlog(
                  clrl_log.error,
                  "Failed to get IMDSv2 token: {}",
                  error_response);
                co_return token_response;
            }
        } else {
            token = read_string_from_response(std::move(token_response));
        }
    }

    if (unlikely(!_role)) {
        vlog(clrl_log.info, "initializing role name");
        auto response = co_await fetch_role_name(token);
        // error while fetching the role, make caller handle it
        if (std::holds_alternative<api_request_error>(response)) {
            co_return response;
        }

        _role.emplace(read_string_from_response(std::move(response)));

        vlog(clrl_log.info, "fetched iam role name [{}]", *_role);
        if (_role->empty()) {
            // TODO (abhijat) create a new error kind for bad system state
            co_return api_request_error{
              .reason = "empty role name set on instance",
              .error_kind = api_request_error_kind::failed_abort};
        }
    }

    if (_role->empty()) {
        vlog(
          clrl_log.error,
          "IAM role name not populated, cannot fetch credentials");
        // TODO (abhijat) create a new error kind for bad system state
        co_return api_request_error{
          .reason = "missing IAM role name",
          .error_kind = api_request_error_kind::failed_retryable};
    }

    co_return co_await make_request(
      co_await make_api_client("aws"),
      imds::get(
        address().host(),
        fmt::format("/latest/meta-data/iam/security-credentials/{}", *_role),
        token));
}

api_response_parse_result aws_refresh_impl::parse_response(iobuf resp) {
    auto doc = parse_json_response(std::move(resp));
    std::vector<ss::sstring> missing_fields;
    for (const auto& key :
         {ec2_response_schema::expiry,
          ec2_response_schema::access_key_id,
          ec2_response_schema::secret_access_key,
          ec2_response_schema::session_token}) {
        if (!doc.HasMember(key.data())) {
            missing_fields.emplace_back(key);
        }
    }

    if (!missing_fields.empty()) {
        return malformed_api_response_error{
          .missing_fields = std::move(missing_fields)};
    }

    auto expiration = doc[ec2_response_schema::expiry.data()].GetString();
    next_sleep_duration(calculate_sleep_duration(parse_timestamp(expiration)));

    return aws_credentials{
      .access_key_id
      = public_key_str{doc[ec2_response_schema::access_key_id.data()]
                         .GetString()},
      .secret_access_key
      = private_key_str{doc[ec2_response_schema::secret_access_key.data()]
                          .GetString()},
      .session_token
      = session_token{doc[ec2_response_schema::session_token.data()]
                        .GetString()},
      .region = region(),
      .service = _service,
    };
}

ss::future<api_response>
aws_refresh_impl::fetch_role_name(std::optional<std::string_view> token) {
    co_return co_await make_request(
      co_await make_api_client("aws"),
      imds::get(
        address().host(),
        "/latest/meta-data/iam/security-credentials/",
        token));
}

ss::future<api_response> aws_refresh_impl::fetch_instance_metadata_token() {
    co_return co_await make_request(
      co_await make_api_client("aws"),
      imds::token_request(address().host(), imds_token_ttl));
}

fmt::iterator aws_refresh_impl::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "aws_refresh_impl{{address:{}}}", address());
}

} // namespace cloud_roles
