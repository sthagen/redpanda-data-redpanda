/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/s3_multipart_state.h"

#include "base/external_fmt.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/s3_client.h"
#include "cloud_storage_clients/s3_client_utils.h"
#include "cloud_storage_clients/s3_error.h"
#include "cloud_storage_clients/util.h"
#include "http/client.h"
#include "http/utils.h"
#include "utils/xml.h"

#include <seastar/core/coroutine.hh>

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>

#include <exception>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace cloud_storage_clients {

namespace {
// Every client in a pool is the same concrete backend type, so this downcast
// of the per-request client is sound; the dassert guards that in debug.
s3_client* as_s3_client(client& c) {
    dassert(
      dynamic_cast<s3_client*>(&c) != nullptr,
      "multipart request issued on a non-s3 client");
    return static_cast<s3_client*>(&c);
}
} // namespace

// s3_multipart_state implementations //

s3_multipart_state::s3_multipart_state(
  ss::shared_ptr<client_provider> provider,
  plain_bucket_name bucket,
  object_key key,
  ss::lowres_clock::duration timeout)
  : _provider(std::move(provider))
  , _bucket(std::move(bucket))
  , _key(std::move(key))
  , _timeout(timeout) {}

ss::future<> s3_multipart_state::initialize_multipart() {
    auto lease = co_await _provider->acquire();
    auto* s3c = as_s3_client(*lease.client);
    vlog(
      s3_log.debug,
      "Initializing S3 multipart upload for {}/{}",
      _bucket,
      _key);

    auto header = s3c->_requestor.make_create_multipart_upload_request(
      _bucket, _key);
    if (!header) {
        throw std::system_error(header.error());
    }

    vlog(
      s3_log.trace, "send CreateMultipartUpload request:\n{}", header.value());

    auto response_stream = co_await s3c->_client.request(
      std::move(header.value()), _timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::ok) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        co_return co_await parse_rest_error_response(
          content_type, status, std::move(buf));
    }

    // Parse XML response to extract UploadId
    auto response_buf = co_await http::drain(std::move(response_stream));

    try {
        auto response_tree = xml::iobuf_to_ptree(
          std::move(response_buf), s3_log);
        _upload_id = xml::get_from_ptree<std::string>(
          response_tree, "InitiateMultipartUploadResult.UploadId");

        s3c->_probe->register_multipart_create();

        vlog(
          s3_log.debug,
          "Initialized S3 multipart upload with upload_id: {}",
          _upload_id);
    } catch (const std::exception& ex) {
        vlog(
          s3_log.error,
          "Failed to parse CreateMultipartUpload response: {}",
          ex);
        throw std::runtime_error(
          fmt::format(
            "Failed to parse UploadId from CreateMultipartUpload response: {}",
            ex.what()));
    }
}

ss::future<> s3_multipart_state::upload_part(size_t part_num, iobuf data) {
    auto lease = co_await _provider->acquire();
    auto* s3c = as_s3_client(*lease.client);
    vassert(!_upload_id.empty(), "Multipart upload not initialized");

    const size_t data_size = data.size_bytes();
    vlog(
      s3_log.debug,
      "Uploading part {} (size: {}) for upload_id: {}",
      part_num,
      data_size,
      _upload_id);

    auto header = s3c->_requestor.make_upload_part_request(
      _bucket, _key, part_num, _upload_id, data_size);
    if (!header) {
        throw std::system_error(header.error());
    }

    vlog(s3_log.trace, "send UploadPart request:\n{}", header.value());

    // Convert iobuf to input_stream
    auto body = make_iobuf_input_stream(std::move(data));

    auto response_stream = co_await s3c->_client
                             .request(std::move(header.value()), body, _timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::ok) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        co_return co_await parse_rest_error_response(
          content_type, status, std::move(buf));
    }

    // Extract ETag from response headers
    const auto& headers = response_stream->get_headers();
    auto etag_it = headers.find(boost::beast::http::field::etag);
    if (etag_it == headers.end()) {
        co_await http::drain(std::move(response_stream));
        throw std::runtime_error("UploadPart response missing ETag header");
    }

    ss::sstring etag(etag_it->value().data(), etag_it->value().size());
    _etags.push_back(std::move(etag));

    // Drain response
    co_await http::drain(std::move(response_stream));

    s3c->_probe->register_multipart_upload();

    vlog(
      s3_log.debug, "Uploaded part {} with ETag: {}", part_num, _etags.back());
}

ss::future<> s3_multipart_state::complete_multipart_upload() {
    auto lease = co_await _provider->acquire();
    auto* s3c = as_s3_client(*lease.client);
    vassert(!_upload_id.empty(), "Multipart upload not initialized");

    vlog(
      s3_log.debug,
      "Completing S3 multipart upload {} with {} parts",
      _upload_id,
      _etags.size());

    auto request = s3c->_requestor.make_complete_multipart_upload_request(
      _bucket, _key, _upload_id, _etags);
    if (!request) {
        throw std::system_error(request.error());
    }

    auto [header, body] = std::move(request.value());
    vlog(s3_log.trace, "send CompleteMultipartUpload request:\n{}", header);

    auto response_stream = co_await s3c->_client
                             .request(std::move(header), body, _timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::ok) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        co_return co_await parse_rest_error_response(
          content_type, status, std::move(buf));
    }

    // AWS S3 can return errors embedded in a 200 OK response body for
    // CompleteMultipartUpload. The response must be parsed to detect these.
    // See:
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    auto response_buf = co_await http::drain(std::move(response_stream));
    auto response_tree = xml::iobuf_to_ptree(std::move(response_buf), s3_log);
    if (
      auto error_code = xml::get_optional_from_ptree<std::string>(
        response_tree, "Error.Code");
      error_code) {
        throw rest_error_response(
          *error_code,
          xml::get_from_ptree<std::string>(response_tree, "Error.Message", ""),
          xml::get_from_ptree<std::string>(
            response_tree, "Error.RequestId", ""),
          xml::get_from_ptree<std::string>(
            response_tree, "Error.Resource", ""));
    }

    s3c->_probe->register_multipart_complete();

    vlog(s3_log.debug, "Completed multipart upload {}", _upload_id);
}

ss::future<> s3_multipart_state::abort_multipart_upload() {
    if (_upload_id.empty()) {
        // Nothing to abort
        vlog(s3_log.debug, "Abort called but multipart not initialized");
        co_return;
    }

    vlog(s3_log.debug, "Aborting S3 multipart upload {}", _upload_id);

    auto lease = co_await _provider->acquire();
    auto* s3c = as_s3_client(*lease.client);

    auto header = s3c->_requestor.make_abort_multipart_upload_request(
      _bucket, _key, _upload_id);
    if (!header) {
        // Log error but don't throw - abort should be best effort
        vlog(
          s3_log.warn,
          "Failed to create abort request: {}",
          header.error().message());
        co_return;
    }

    vlog(
      s3_log.trace, "send AbortMultipartUpload request:\n{}", header.value());

    try {
        auto response_stream = co_await s3c->_client.request(
          std::move(header.value()), _timeout);

        co_await response_stream->prefetch_headers();
        vassert(response_stream->is_header_done(), "Header is not received");

        const auto status = response_stream->get_headers().result();
        if (
          status != boost::beast::http::status::no_content
          && status != boost::beast::http::status::ok) {
            vlog(
              s3_log.warn,
              "AbortMultipartUpload returned unexpected status: {}",
              status);
        }

        // Drain response
        co_await http::drain(std::move(response_stream));

        s3c->_probe->register_multipart_abort();

        vlog(s3_log.debug, "Aborted multipart upload {}", _upload_id);
    } catch (const std::exception& ex) {
        // Log but don't throw - abort failures are non-fatal
        vlog(
          s3_log.warn,
          "Failed to abort multipart upload {}: {}",
          _upload_id,
          ex);
    }
}

ss::future<> s3_multipart_state::upload_as_single_object(iobuf data) {
    auto lease = co_await _provider->acquire();
    auto* s3c = as_s3_client(*lease.client);
    vlog(
      s3_log.debug,
      "Using single put_object for small file (size: {})",
      data.size_bytes());

    const size_t data_size = data.size_bytes();
    auto body = make_iobuf_input_stream(std::move(data));

    // Use existing put_object implementation
    co_await s3c->do_put_object(
      _bucket, _key, data_size, std::move(body), _timeout);
}

} // namespace cloud_storage_clients
