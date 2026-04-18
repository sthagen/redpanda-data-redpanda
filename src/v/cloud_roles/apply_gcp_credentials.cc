/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "apply_gcp_credentials.h"

#include "base/vlog.h"

namespace cloud_roles {
apply_gcp_credentials::apply_gcp_credentials(gcp_credentials credentials)
  : _oauth_token{fmt::format("Bearer {}", credentials.oauth_token())} {}

std::error_code
apply_gcp_credentials::add_auth(http::client::request_header& header) const {
    auto token = _oauth_token();
    header.insert(
      boost::beast::http::field::authorization, {token.data(), token.size()});
    return {};
}

void apply_gcp_credentials::reset_creds(credentials creds) {
    if (!std::holds_alternative<gcp_credentials>(creds)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "credential applier reset with incorrect credential type {}",
          creds));
    }
    _oauth_token = oauth_token_str{
      fmt::format("Bearer {}", std::get<gcp_credentials>(creds).oauth_token())};
}

fmt::iterator apply_gcp_credentials::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "apply_gcp_credentials");
}

} // namespace cloud_roles
