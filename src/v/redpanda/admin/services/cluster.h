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

#include "features/fwd.h"
#include "proto/redpanda/core/admin/v2/cluster.proto.h"
#include "redpanda/admin/kafka_connections_service.h"
#include "redpanda/admin/proxy/client.h"

namespace admin {

class cluster_service_impl : public proto::admin::cluster_service {
public:
    cluster_service_impl(
      admin::proxy::client,
      ss::sharded<kafka_connections_service>& kafka_connections_service,
      ss::sharded<features::feature_table>& feature_table);

    ss::future<proto::admin::list_kafka_connections_response>
      list_kafka_connections(
        serde::pb::rpc::context,
        proto::admin::list_kafka_connections_request) override;

private:
    admin::proxy::client _proxy_client;
    ss::sharded<kafka_connections_service>& _kafka_connections_service;
    ss::sharded<features::feature_table>& _feature_table;
};

} // namespace admin
