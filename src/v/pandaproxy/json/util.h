/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "pandaproxy/logger.h"
#include "utils/truncating_logger.h"

#include <seastar/http/request.hh>
#include <seastar/net/inet_address.hh>

#include <string_view>

namespace pandaproxy {

inline void log_request(
  const ss::http::request& req, std::string_view body, truncating_logger& log) {
    if (log.is_enabled(ss::log_level::trace)) {
        vlog(
          log.trace,
          "[{}:{}] handling {} {}: body={:?}",
          req.get_client_address().addr(),
          req.get_client_address().port(),
          req._method,
          req._url,
          body);
    }
}

inline void log_request(
  const ss::http::request& req, const iobuf& body, truncating_logger& log) {
    if (log.is_enabled(ss::log_level::trace)) {
        iobuf_const_parser parser{body};
        log_request(
          req,
          parser.read_string(std::min(parser.bytes_left(), max_log_line_bytes)),
          log);
    }
}

} // namespace pandaproxy
