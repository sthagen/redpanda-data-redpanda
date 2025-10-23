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
#include "kafka/server/fwd.h"
#include "proto/redpanda/core/admin/v2/cluster.proto.h"
#include "redpanda/admin/proxy/client.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <cstddef>

namespace admin {

class kafka_connections_service {
public:
    explicit kafka_connections_service(ss::sharded<kafka::server>& kafka_server)
      : _kafka_server(kafka_server) {}

    // List connections from all shards on this node
    ss::future<proto::admin::list_kafka_connections_response>
    list_kafka_connections_local(
      proto::admin::list_kafka_connections_request req);

    // List connections from all nodes and shard in the cluster
    ss::future<proto::admin::list_kafka_connections_response>
    list_kafka_connections_cluster_wide(
      admin::proxy::client& proxy_client,
      const serde::pb::rpc::context& ctx,
      proto::admin::list_kafka_connections_request req);

    static size_t get_effective_limit(size_t page_size);

private:
    ss::sharded<kafka::server>& _kafka_server;
};

} // namespace admin
