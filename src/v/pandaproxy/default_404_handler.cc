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

#include "pandaproxy/default_404_handler.h"

#include "pandaproxy/error.h"
#include "pandaproxy/json/requests/error_reply.h"
#include "pandaproxy/json/rjson_util.h"

#include <seastar/core/future.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

#include <memory>
#include <utility>

namespace pandaproxy {

ss::future<std::unique_ptr<ss::http::reply>> default_404_handler::handle(
  const ss::sstring&,
  std::unique_ptr<ss::http::request>,
  std::unique_ptr<ss::http::reply> rep) {
    auto ec = make_error_condition(reply_error_code::not_found);
    json::error_body body{.ec = ec, .message = ss::sstring{ec.message()}};
    rep->set_status(ss::http::reply::status_type::not_found);
    rep->write_body(
      _mime_type, pandaproxy::json::rjson_serialize_str(std::move(body)));
    return ss::make_ready_future<std::unique_ptr<ss::http::reply>>(
      std::move(rep));
}

} // namespace pandaproxy
