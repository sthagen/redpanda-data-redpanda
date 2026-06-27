/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/s3_client_utils.h"

#include "base/external_fmt.h"
#include "base/units.h"
#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage_clients/logger.h"
#include "strings/utf8.h"
#include "utils/xml.h"

#include <exception>

namespace cloud_storage_clients {

s3_error_code status_to_error_code(boost::beast::http::status s) {
    // According to this https://repost.aws/knowledge-center/http-5xx-errors-s3
    // the 500 and 503 errors are used in case of throttling
    if (
      s == boost::beast::http::status::service_unavailable
      || s == boost::beast::http::status::internal_server_error) {
        return s3_error_code::slow_down;
    }
    return s3_error_code::_unknown;
}

rest_error_response parse_xml_rest_error_response(iobuf&& buf) {
    try {
        auto resp = xml::iobuf_to_ptree(std::move(buf), s3_log);
        constexpr const char* empty = "";
        auto code = xml::get_from_ptree<std::string>(resp, "Error.Code", empty);
        auto msg = xml::get_from_ptree<std::string>(
          resp, "Error.Message", empty);
        auto rid = xml::get_from_ptree<std::string>(
          resp, "Error.RequestId", empty);
        auto res = xml::get_from_ptree<std::string>(
          resp, "Error.Resource", empty);
        rest_error_response err(code, msg, rid, res);
        return err;
    } catch (...) {
        vlog(s3_log.error, "!!error parse error {}", std::current_exception());
        throw;
    }
}

rest_error_response parse_unknown_format_error_response(
  boost::beast::http::status status, iobuf buf) {
    static constexpr auto max_body_size = static_cast<size_t>(4_KiB);
    static constexpr auto truncation_warning_segment = " [truncated~4096]";

    auto maybe_truncation_warning = "";
    auto read_size = buf.size_bytes();

    // if the error message exceeds reasonable size (4k), truncate it and
    // indicate a truncation to the recipient
    if (read_size > max_body_size) {
        maybe_truncation_warning = truncation_warning_segment;
        read_size = max_body_size;
    }

    ss::sstring error_body = "";
    iobuf_parser parser{std::move(buf)};

    // this is the unhappy unhappy path, handle potentially invalid utf8
    try {
        error_body = parser.read_string(read_size);
    } catch (const invalid_utf8_exception& e) {
        vlog(s3_log.info, "response body contains invalid utf8 {}", e);
    } catch (...) {
        vlog(s3_log.error, "error parse error {}", std::current_exception());
        throw;
    }

    return rest_error_response{
      fmt::format("{}", status_to_error_code(status)),
      fmt::format(
        "http status: {}, error body{}: {}",
        status,
        maybe_truncation_warning,
        error_body),
      "",
      ""};
}

} // namespace cloud_storage_clients
