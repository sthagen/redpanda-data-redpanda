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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "redpanda/admin/proxy/proxy_service.h"
#include "serde/protobuf/rpc.h"

#include <seastar/util/noncopyable_function.hh>

namespace admin::proxy {

// A handler that can use the context to handle the request after parsing the
// provided payload as the proper protobuf. Then the result is the serialized
// protobuf response.
using generic_handler
  = ss::noncopyable_function<ss::future<iobuf>(serde::pb::rpc::context, iobuf)>;

// The implementation of a proxy service that can handle internally proxied
// admin RPC requests using our internal RPC protocol.
//
// This class is a thin bridge between the admin server and our internal RPCs.
class service_impl : public proxy_service {
public:
    service_impl(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      generic_handler handler)
      : proxy_service(sc, ssg)
      , _handler(std::move(handler)) {}

    service_impl(const service_impl&) = delete;
    service_impl(service_impl&&) = delete;
    service_impl& operator=(const service_impl&) = delete;
    service_impl& operator=(service_impl&&) = delete;
    ~service_impl() noexcept override = default;

    ss::future<proxy_response>
    proxy_rpc(proxy_request, rpc::streaming_context&) override;

private:
    generic_handler _handler;
};

} // namespace admin::proxy
