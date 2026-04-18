/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/external_fmt.h"
#include "base/format_to.h"
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>

namespace cloud_roles {

inline constexpr auto retryable_system_error_codes = std::to_array(
  {ECONNREFUSED, ENETUNREACH, ETIMEDOUT, ECONNRESET, EPIPE});

bool is_retryable(const std::system_error& ec);

inline constexpr auto retryable_http_status = std::to_array({
  boost::beast::http::status::request_timeout,
  boost::beast::http::status::gateway_timeout,
  boost::beast::http::status::bad_gateway,
  boost::beast::http::status::service_unavailable,
  boost::beast::http::status::internal_server_error,
  boost::beast::http::status::network_connect_timeout_error,
});

bool is_retryable(boost::beast::http::status status);

enum class api_request_error_kind { failed_abort, failed_retryable };

inline fmt::iterator format_to(api_request_error_kind kind, fmt::iterator out) {
    switch (kind) {
    case api_request_error_kind::failed_abort:
        return fmt::format_to(out, "failed_abort");
    case api_request_error_kind::failed_retryable:
        return fmt::format_to(out, "failed_retryable");
    }
}

struct api_request_error {
    boost::beast::http::status status{boost::beast::http::status::ok};
    ss::sstring reason;
    api_request_error_kind error_kind;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "api_request_error{{reason:{}, error_kind:{}}}",
          reason,
          error_kind);
    }
};

api_request_error make_abort_error(const std::exception& ex);
api_request_error
make_abort_error(ss::sstring reason, boost::beast::http::status status);

api_request_error make_retryable_error(const std::exception& ex);
api_request_error
make_retryable_error(ss::sstring reason, boost::beast::http::status status);

using api_response = std::variant<iobuf, api_request_error>;

struct malformed_api_response_error {
    std::vector<ss::sstring> missing_fields;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "malformed_api_response_error{{missing_fields:{}}}",
          missing_fields);
    }
};

struct api_response_parse_error {
    ss::sstring reason;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it, "api_response_parse_error{{reason:{}}}", reason);
    }
};

using oauth_token_str = named_type<ss::sstring, struct oauth_token_str_tag>;

struct gcp_credentials {
    oauth_token_str oauth_token;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it, "gcp_credentials{{oauth_token:**{}**}}", oauth_token().size());
    }
};

using aws_service_name = named_type<ss::sstring, struct aws_service_name_>;
using aws_region_name = named_type<ss::sstring, struct aws_region_name_>;
using public_key_str = named_type<ss::sstring, struct public_key_str_>;
using private_key_str = named_type<ss::sstring, struct private_key_str_>;
using timestamp = std::chrono::time_point<std::chrono::system_clock>;
using session_token = named_type<ss::sstring, struct session_token_str_>;
using storage_account = named_type<ss::sstring, struct storage_account_tag>;

struct aws_credentials {
    public_key_str access_key_id;
    private_key_str secret_access_key;
    std::optional<session_token> session_token;
    aws_region_name region;
    aws_service_name service;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "aws_credentials{{access_key_id: **{}**, secret_access_key: "
          "**{}**, "
          "session_token: **{}**, region: {}, service: {}}}",
          access_key_id().size(),
          secret_access_key().size(),
          session_token.value_or(cloud_roles::session_token{})().size(),
          region(),
          service());
    }
};

struct abs_credentials {
    storage_account storage_account;
    private_key_str shared_key;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "abs_credentials{{storage_account: **{}**, shared_key: **{}**}}",
          storage_account().size(),
          shared_key().size());
    }
};

// AKS federated OpenID Credentials uses Azure's managed identities to retrieve
// a Oauth authorization token
struct abs_oauth_credentials {
    oauth_token_str oauth_token;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "abs_oauth_credentials{{oauth_token:**{}**}}",
          oauth_token().size());
    }
};

using credentials = std::variant<
  aws_credentials,
  gcp_credentials,
  abs_credentials,
  abs_oauth_credentials>;

// tmp trick to ensure that we are not calling into infinite recursion if
// there is a new credential but no format_to
template<std::same_as<credentials> Cred>
std::ostream& operator<<(std::ostream& os, const Cred& c);

using api_response_parse_result = std::variant<
  malformed_api_response_error,
  api_response_parse_error,
  api_request_error,
  credentials>;

using credentials_update_cb_t
  = ss::noncopyable_function<ss::future<>(credentials)>;

// Azure expects every request to Blob Storage to contain an
// 'x-ms-version' header that specifies the API version to use.
// This version is hardcoded in Redpanda to ensure that an API
// version that we've tested with is used in field.
// https://learn.microsoft.com/en-us/rest/api/storageservices/version-2023-01-03

// Update this version to use a different storage API version, for example when
// adding support to a new functionality released after the current version
// 2023-01-03.
inline constexpr auto azure_storage_api_version = "2023-01-03";

} // namespace cloud_roles
