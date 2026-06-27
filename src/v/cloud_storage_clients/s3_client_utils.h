/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/external_fmt.h"
#include "bytes/iobuf.h"
#include "cloud_storage_clients/s3_error.h"
#include "cloud_storage_clients/util.h"

#include <seastar/core/future.hh>

#include <boost/beast/http/status.hpp>

namespace cloud_storage_clients {

s3_error_code status_to_error_code(boost::beast::http::status s);

rest_error_response parse_xml_rest_error_response(iobuf&& buf);

rest_error_response parse_unknown_format_error_response(
  boost::beast::http::status status, iobuf buf);

template<typename ResultT = void>
ss::future<ResultT> parse_rest_error_response(
  response_content_type type, boost::beast::http::status result, iobuf buf) {
    // AWS errors occasionally come with an empty body
    // (See https://github.com/redpanda-data/redpanda/issues/6061)
    // Without a proper code, we treat it as a hint to gracefully retry
    // (synthesize the slow_down code).
    if (!buf.empty()) {
        if (type == response_content_type::xml) {
            // Error responses from S3 _should_ have the Content-Type header set
            // with `application/xml`- however, certain responses (such as `503
            // Service Unavailable`) may not be of this form.
            // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
            return ss::make_exception_future<ResultT>(
              parse_xml_rest_error_response(std::move(buf)));
        } else {
            // render out up to 4k of plaintext or json errors
            return ss::make_exception_future<ResultT>(
              parse_unknown_format_error_response(result, std::move(buf)));
        }
    }

    return ss::make_exception_future<ResultT>(rest_error_response(
      fmt::format("{}", status_to_error_code(result)),
      fmt::format("Empty error response, status code {}", result),
      "",
      ""));
}

} // namespace cloud_storage_clients
