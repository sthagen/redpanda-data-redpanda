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

#include "cloud_instance_metadata/aws_imds.h"

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/verb.hpp>
#include <fmt/format.h>

namespace cloud_instance_metadata::aws_imds {

namespace {

boost::beast::http::request_header<> base_request(
  std::string_view host,
  boost::beast::http::verb method,
  std::string_view target) {
    boost::beast::http::request_header<> req;
    req.insert(boost::beast::http::field::host, {host.data(), host.size()});
    req.method(method);
    req.target({target.data(), target.size()});
    return req;
}

} // namespace

boost::beast::http::request_header<>
token_request(std::string_view host, std::chrono::seconds token_ttl) {
    auto req = base_request(host, boost::beast::http::verb::put, token_path);
    req.insert(
      {token_ttl_header.data(), token_ttl_header.size()},
      fmt::format("{}", token_ttl.count()));
    return req;
}

boost::beast::http::request_header<> get(
  std::string_view host,
  std::string_view path,
  std::optional<std::string_view> token) {
    auto req = base_request(host, boost::beast::http::verb::get, path);
    if (token.has_value()) {
        req.insert(
          {token_header.data(), token_header.size()},
          {token->data(), token->size()});
    }
    return req;
}

} // namespace cloud_instance_metadata::aws_imds
