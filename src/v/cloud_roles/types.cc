/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/types.h"

#include <seastar/util/variant_utils.hh>

namespace cloud_roles {

// tmp trick to ensure that we are not calling into infinite recursion if
// there is a new credential but no format_to
template<std::same_as<credentials> Cred>
std::ostream& operator<<(std::ostream& os, const Cred& c) {
    ss::visit(c, [&os](const auto& creds) { os << creds; });
    return os;
}

template std::ostream& operator<<(std::ostream& os, const credentials& c);

bool is_retryable(const std::system_error& ec) {
    auto code = ec.code();
    return std::find(
             retryable_system_error_codes.begin(),
             retryable_system_error_codes.end(),
             code.value())
           != retryable_system_error_codes.end();
}

bool is_retryable(boost::beast::http::status status) {
    return std::find(
             retryable_http_status.begin(), retryable_http_status.end(), status)
           != retryable_http_status.end();
}

api_request_error make_abort_error(const std::exception& ex) {
    return api_request_error{
      .reason = ex.what(),
      .error_kind = api_request_error_kind::failed_abort,
    };
}

api_request_error
make_abort_error(ss::sstring reason, boost::beast::http::status status) {
    return api_request_error{
      .status = status,
      .reason = reason,
      .error_kind = api_request_error_kind::failed_abort,
    };
}

api_request_error make_retryable_error(const std::exception& ex) {
    return api_request_error{
      .reason = ex.what(),
      .error_kind = api_request_error_kind::failed_retryable,
    };
}

api_request_error
make_retryable_error(ss::sstring reason, boost::beast::http::status status) {
    return api_request_error{
      .status = status,
      .reason = reason,
      .error_kind = api_request_error_kind::failed_retryable};
}

} // namespace cloud_roles
