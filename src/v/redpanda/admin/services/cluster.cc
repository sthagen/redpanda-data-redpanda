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

#include "redpanda/admin/services/cluster.h"

#include "features/feature_table.h"
#include "proto/redpanda/core/admin/v2/cluster.proto.h"
#include "redpanda/admin/kafka_connections_service.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
} // namespace proto

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger brlog{"admin_api_server/cluster_service"};

} // namespace

cluster_service_impl::cluster_service_impl(
  admin::proxy::client client,
  ss::sharded<kafka_connections_service>& kafka_connections_service,
  ss::sharded<features::feature_table>& feature_table)
  : _proxy_client(std::move(client))
  , _kafka_connections_service(kafka_connections_service)
  , _feature_table(feature_table) {}

ss::future<proto::admin::list_kafka_connections_response>
cluster_service_impl::list_kafka_connections(
  serde::pb::rpc::context ctx,
  proto::admin::list_kafka_connections_request req) {
    vlog(brlog.trace, "list_kafka_connections: {}", req);

    utils::check_license(_feature_table.local());

    auto& kcs = _kafka_connections_service.local();
    auto resp = ctx.is_proxied()
                  ? co_await kcs.list_kafka_connections_local(std::move(req))
                  : co_await kcs.list_kafka_connections_cluster_wide(
                      _proxy_client, ctx, std::move(req));

    vlog(
      brlog.trace,
      "list_kafka_connections: response connections: {} ({}b), total matching: "
      "{}",
      resp.get_connections().size(),
      resp.get_connections().memory_size(),
      resp.get_total_size());

    co_return resp;
}

} // namespace admin
