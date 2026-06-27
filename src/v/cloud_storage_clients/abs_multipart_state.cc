/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/abs_multipart_state.h"

#include "base/external_fmt.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage_clients/abs_client.h"
#include "cloud_storage_clients/abs_client_utils.h"
#include "cloud_storage_clients/abs_error.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/util.h"
#include "http/client.h"
#include "http/utils.h"
#include "utils/base64.h"

#include <seastar/core/coroutine.hh>

#include <boost/beast/http/status.hpp>

#include <exception>
#include <system_error>
#include <utility>

namespace cloud_storage_clients {

namespace {

// Helper function to generate Base64-encoded block IDs for ABS multipart upload
// Block IDs must all be the same pre-encoded length, so we use 10-digit
// zero-padded format
ss::sstring generate_block_id(size_t part_number) {
    auto id = fmt::format("{:010d}", part_number);
    // Convert to bytes_view for Base64 encoding
    bytes_view bv{reinterpret_cast<const uint8_t*>(id.data()), id.size()};
    return bytes_to_base64(bv);
}

// Every client in a pool is the same concrete backend type, so this downcast
// of the per-request client is sound; the dassert guards that in debug.
abs_client* as_abs_client(client& c) {
    dassert(
      dynamic_cast<abs_client*>(&c) != nullptr,
      "multipart request issued on a non-abs client");
    return static_cast<abs_client*>(&c);
}

} // namespace

// abs_multipart_state implementation

abs_multipart_state::abs_multipart_state(
  ss::shared_ptr<client_provider> provider,
  plain_bucket_name container,
  object_key key,
  ss::lowres_clock::duration timeout)
  : _provider(std::move(provider))
  , _container(std::move(container))
  , _key(std::move(key))
  , _timeout(timeout) {}

ss::future<> abs_multipart_state::initialize_multipart() {
    auto lease = co_await _provider->acquire();
    auto* absc = as_abs_client(*lease.client);
    // ABS Block Blobs don't require initialization - blocks can be uploaded
    // directly
    vlog(abs_log.debug, "ABS multipart upload initialized (no-op)");
    _initialized = true;
    absc->_probe->register_multipart_create();
    co_return;
}

ss::future<> abs_multipart_state::upload_part(size_t part_num, iobuf data) {
    auto lease = co_await _provider->acquire();
    auto* absc = as_abs_client(*lease.client);
    // Generate Base64-encoded block ID
    auto block_id = generate_block_id(part_num);

    vlog(
      abs_log.debug,
      "Uploading ABS block {} (block_id: {}, size: {})",
      part_num,
      block_id,
      data.size_bytes());

    // Create Put Block request
    auto header = absc->_requestor.make_put_block_request(
      _container, _key, block_id, data.size_bytes());
    if (!header) {
        vlog(
          abs_log.error,
          "Failed to create Put Block request: {}",
          header.error());
        throw std::system_error(header.error());
    }

    // Upload the block
    auto body = make_iobuf_input_stream(std::move(data));
    auto response_stream = co_await absc->_client
                             .request(std::move(header.value()), body, _timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::created) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    co_await http::drain(std::move(response_stream));

    absc->_probe->register_multipart_upload();

    _block_ids.push_back(block_id);
}

ss::future<> abs_multipart_state::complete_multipart_upload() {
    auto lease = co_await _provider->acquire();
    auto* absc = as_abs_client(*lease.client);
    vlog(
      abs_log.debug,
      "Completing ABS multipart upload ({} blocks)",
      _block_ids.size());

    // Create Put Block List request
    auto put_block_list_req = absc->_requestor.make_put_block_list_request(
      _container, _key, _block_ids);
    if (!put_block_list_req) {
        throw std::system_error(put_block_list_req.error());
    }
    auto [header, body] = std::move(put_block_list_req.value());

    // Commit the blocks
    auto response_stream = co_await absc->_client
                             .request(std::move(header), body, _timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::created) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    co_await http::drain(std::move(response_stream));

    absc->_probe->register_multipart_complete();
}

ss::future<> abs_multipart_state::abort_multipart_upload() {
    auto lease = co_await _provider->acquire();
    auto* absc = as_abs_client(*lease.client);
    // ABS uncommitted blocks expire after 7 days - no explicit abort needed
    vlog(abs_log.debug, "ABS multipart upload aborted (no-op)");
    absc->_probe->register_multipart_abort();
    co_return;
}

ss::future<> abs_multipart_state::upload_as_single_object(iobuf data) {
    auto lease = co_await _provider->acquire();
    auto* absc = as_abs_client(*lease.client);
    auto size = data.size_bytes();
    vlog(
      abs_log.debug,
      "ABS small file optimization: using Put Blob (size: {})",
      size);

    // Use the regular put_object method for small files
    auto body = make_iobuf_input_stream(std::move(data));
    co_await absc->do_put_object(
      _container, _key, size, std::move(body), _timeout);
}

} // namespace cloud_storage_clients
