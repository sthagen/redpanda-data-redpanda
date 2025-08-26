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

#include "redpanda/admin/proxy/service.h"

#include "base/vlog.h"
#include "serde/protobuf/rpc.h"

#include <exception>

namespace admin::proxy {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,*-err58-cpp)
ss::logger log{"admin/proxy/service"};
} // namespace

ss::future<proxy_response>
service_impl::proxy_rpc(proxy_request req, rpc::streaming_context&) {
    serde::pb::rpc::context ctx{
      .service_name = req.service,
      .method_name = req.method,
      .content_type = serde::pb::rpc::content_type::proto,
      .proxied_nodes = req.via,
    };
    proxy_response response;
    try {
        auto payload = co_await _handler(
          std::move(ctx), std::move(req.payload));
        response.error_code = errc::ok;
        response.payload = std::move(payload);
        co_return response;
    } catch (const serde::pb::rpc::cancelled_exception& e) {
        response.error_code = errc::cancelled;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::unknown_exception& e) {
        response.error_code = errc::unknown;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::invalid_argument_exception& e) {
        response.error_code = errc::invalid_argument;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::deadline_exceeded_exception& e) {
        response.error_code = errc::deadline_exceeded;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::not_found_exception& e) {
        response.error_code = errc::not_found;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::already_exists_exception& e) {
        response.error_code = errc::already_exists;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::permission_denied_exception& e) {
        response.error_code = errc::permission_denied;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::resource_exhausted_exception& e) {
        response.error_code = errc::resource_exhausted;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::failed_precondition_exception& e) {
        response.error_code = errc::failed_precondition;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::aborted_exception& e) {
        response.error_code = errc::aborted;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::out_of_range_exception& e) {
        response.error_code = errc::out_of_range;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::unimplemented_exception& e) {
        response.error_code = errc::unimplemented;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::internal_exception& e) {
        response.error_code = errc::internal_error;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::unavailable_exception& e) {
        response.error_code = errc::unavailable;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::data_loss_exception& e) {
        response.error_code = errc::data_loss;
        response.payload = iobuf::from(e.what());
    } catch (const serde::pb::rpc::unauthenticated_exception& e) {
        response.error_code = errc::unauthenticated;
        response.payload = iobuf::from(e.what());
    } catch (...) {
        vlog(
          log.warn,
          "unexpected error handling RPC {}.{}: {}",
          req.service,
          req.method,
          std::current_exception());
        response.error_code = errc::internal_error;
    }
    co_return response;
}

} // namespace admin::proxy
