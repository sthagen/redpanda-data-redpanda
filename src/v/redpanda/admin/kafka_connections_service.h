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
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <cstddef>
#include <memory>

namespace admin {

class kafka_connections_service
  : public ss::peering_sharded_service<kafka_connections_service> {
public:
    using remote_units = ss::foreign_ptr<std::unique_ptr<ssx::semaphore_units>>;
    constexpr static auto rate_limiter_shard = ss::shard_id{0};

    explicit kafka_connections_service(ss::sharded<kafka::server>&);

    ss::future<> start(ssx::semaphore& admin_memory_semaphore);

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

    size_t get_effective_limit(size_t page_size);

    ss::future<remote_units> rate_limit();

    ss::future<std::vector<remote_units>> memory_limit(size_t connection_count);

private:
    ss::future<remote_units> do_rate_limit();

    ss::future<remote_units> do_memory_limit(size_t connection_count);

    ss::sharded<kafka::server>& _kafka_server;
    std::unique_ptr<ssx::semaphore> _rate_limiter{};
    ssx::semaphore* _admin_memory_semaphore{};
    size_t _admin_memory_semaphore_max{};
};

} // namespace admin
