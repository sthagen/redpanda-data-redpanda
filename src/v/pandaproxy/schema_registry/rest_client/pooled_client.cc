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
#include "pandaproxy/schema_registry/rest_client/pooled_client.h"

#include "base/vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/as_future.hh>

#include <utility>

namespace pandaproxy::schema_registry::rest_client {

pooled_client::pooled_client(
  std::vector<std::unique_ptr<http::abstract_client>> transports)
  : _transports(std::move(transports))
  , _slots(_transports.size(), "schema_registry/rest_client/pool") {
    vassert(
      !_transports.empty(), "a pooled_client requires at least one transport");
    _idle.reserve(_transports.size());
    for (const auto& transport : _transports) {
        _idle.push_back(transport.get());
    }
}

ss::future<http::downloaded_response>
pooled_client::request_and_collect_response(
  boost::beast::http::request_header<>&& request,
  std::optional<iobuf> payload,
  ss::lowres_clock::duration timeout) {
    // Move the request into this frame before any suspension so it does not
    // depend on the caller's object while queued for a transport.
    auto owned_request = std::move(request);
    auto holder = _gate.hold();
    auto slot = co_await ss::get_units(_slots, 1, _as);
    vassert(!_idle.empty(), "slot acquired with no idle transport");
    auto* transport = _idle.back();
    _idle.pop_back();
    auto response = co_await ss::coroutine::as_future(
      transport->request_and_collect_response(
        std::move(owned_request), std::move(payload), timeout));
    // Returned on failure too: the transport reconnects lazily on next use.
    _idle.push_back(transport);
    co_return co_await std::move(response);
}

ss::future<> pooled_client::shutdown_and_stop() {
    auto drained = _gate.close();
    _as.request_abort();
    // Stopping a transport promptly fails a request it is servicing, so
    // in-flight requests unwind and the gate can drain.
    co_await ss::parallel_for_each(
      _transports, [](const std::unique_ptr<http::abstract_client>& transport) {
          return transport->shutdown_and_stop();
      });
    co_await std::move(drained);
}

} // namespace pandaproxy::schema_registry::rest_client
