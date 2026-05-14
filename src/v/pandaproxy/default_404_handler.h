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

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

#include <memory>
#include <utility>

namespace pandaproxy {

/// \brief Default handler for unmatched (method, path) combinations on a
/// pandaproxy::server. Emits the schema-registry/REST-proxy error body
/// {"error_code": 404, "message": "HTTP 404 Not Found"} (with error_code
/// instead of Seastar's fallback code field) so clients parsing this
/// envelope get the shape they expect.
class default_404_handler : public ss::httpd::handler_base {
public:
    explicit default_404_handler(ss::sstring mime_type)
      : _mime_type(std::move(mime_type)) {}

    ss::future<std::unique_ptr<ss::http::reply>> handle(
      const ss::sstring&,
      std::unique_ptr<ss::http::request>,
      std::unique_ptr<ss::http::reply> rep) final;

private:
    ss::sstring _mime_type;
};

} // namespace pandaproxy
