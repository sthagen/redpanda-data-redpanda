/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/metadata.h"
#include "outcome.h"
#include "rpc/backoff_policy.h"
#include "rpc/transport.h"
#include "rpc/types.h"
#include "ssx/semaphore.h"
#include "ssx/sformat.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/socket_defs.hh>

namespace rpc {

/**
 * Provides an interface to get a connected rpc::transport, transparently
 * reconnecting if the underlying transport has become invalid.
 */
class reconnect_transport {
public:
    // Instantiates an underlying rpc::transport, using the given node ID (if
    // provided) to distinguish client metrics that target the same server and
    // to indicate that the client metrics should be aggregated by node ID.
    explicit reconnect_transport(
      rpc::transport_configuration c,
      backoff_policy backoff_policy,
      const std::optional<connection_cache_label>& label = std::nullopt,
      const std::optional<model::node_id>& node_id = std::nullopt)
      : _transport(std::move(c), std::move(label), std::move(node_id))
      , _backoff_policy(std::move(backoff_policy)) {}

    bool is_valid() const { return _transport.is_valid(); }

    rpc::transport& get() { return _transport; }

    /// safe client connect - attempts to reconnect if not connected
    ss::future<result<transport*>> get_connected(clock_type::time_point);
    ss::future<result<transport*>> get_connected(clock_type::duration);

    const net::unresolved_address& server_address() const {
        return _transport.server_address();
    }

    void reset_backoff() { _backoff_policy.reset(); }

    ss::future<> stop();

private:
    ss::future<result<rpc::transport*>> reconnect(clock_type::time_point);
    ss::future<result<rpc::transport*>> reconnect(clock_type::duration);

    rpc::transport _transport;
    rpc::clock_type::time_point _stamp{rpc::clock_type::now()};
    ssx::semaphore _connected_sem{1, "raft/connected"};
    ss::gate _dispatch_gate;
    backoff_policy _backoff_policy;
};
} // namespace rpc
