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
#include "bytes/iobuf.h"
#include "http/client.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <memory>
#include <optional>
#include <vector>

namespace pandaproxy::schema_registry::rest_client {

/// An http::abstract_client that multiplexes requests over a fixed set of
/// underlying transports, so up to pool-size requests run concurrently. Exists
/// because one http::client owns a single connection that cannot service
/// concurrent requests: each request gets exclusive use of one transport for
/// its full duration, and callers beyond the pool size queue (FIFO) for the
/// next free transport.
///
/// A transport is returned to the pool after failure as well as success: a
/// transport whose request failed self-heals on next use (http::client
/// reconnects a stale or dead socket lazily).
///
/// shutdown_and_stop() must be called exactly once before destruction. It
/// rejects new requests, ejects queued waiters, stops every transport (which
/// promptly fails requests that are mid-flight), and drains.
class pooled_client final : public http::abstract_client {
public:
    /// \param transports the single-connection transports to pool; must be
    ///        non-empty. Owned by the pool for its lifetime.
    explicit pooled_client(
      std::vector<std::unique_ptr<http::abstract_client>> transports);

    ss::future<http::downloaded_response> request_and_collect_response(
      boost::beast::http::request_header<>&& request,
      std::optional<iobuf> payload = std::nullopt,
      ss::lowres_clock::duration timeout = http::default_connect_timeout) final;

    ss::future<> shutdown_and_stop() final;

private:
    std::vector<std::unique_ptr<http::abstract_client>> _transports;
    // Transports not currently leased to a request. Borrows from _transports:
    // a request pops one after acquiring a slot and pushes it back when the
    // transport call completes. Capacity is reserved up front so the push
    // back cannot throw.
    std::vector<http::abstract_client*> _idle;
    ssx::semaphore _slots;
    // Ejects slot waiters on shutdown.
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace pandaproxy::schema_registry::rest_client
