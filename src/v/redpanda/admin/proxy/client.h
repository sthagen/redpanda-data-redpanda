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

#include "absl/container/flat_hash_map.h"
#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "rpc/connection_cache.h"
#include "serde/protobuf/rpc.h"

namespace admin::proxy {

// This is a generic client to proxy any ConnectRPC request to another node in
// the cluster. It can be used to forward mutations to the correct leader, or it
// can be used to do scatter-gather requests to all nodes in the cluster.
class client {
public:
    client(
      model::node_id self,
      ss::sharded<rpc::connection_cache>* conn_cache,
      std::function<std::vector<model::node_id>()> all_node_ids_fn)
      : _self(self)
      , _conn_cache(conn_cache)
      , _all_node_ids_fn(std::move(all_node_ids_fn)) {}

    // Create an RPC client for the given node, which can be used to
    // proxy requests to that node.
    template<typename ProtobufServiceClient>
    ProtobufServiceClient make_client_for_node(model::node_id node) {
        return ProtobufServiceClient(
          [this, node](serde::pb::rpc::context ctx, iobuf payload) {
              return send(node, std::move(ctx), std::move(payload));
          });
    }

    // Create an RPC client for every other node in the cluster.
    template<typename ProtobufServiceClient>
    absl::flat_hash_map<model::node_id, ProtobufServiceClient>
    make_clients_for_other_nodes() {
        auto node_ids = _all_node_ids_fn();
        absl::flat_hash_map<model::node_id, ProtobufServiceClient> clients;
        clients.reserve(node_ids.size() - 1);
        for (const auto& node : node_ids) {
            if (node == self_node_id()) {
                continue;
            }
            clients.emplace(
              node, make_client_for_node<ProtobufServiceClient>(node));
        }
        return clients;
    }

    model::node_id self_node_id() const noexcept { return _self; }

private:
    // Send a request to the target node, using the provided context.
    //
    // Returns the response as an iobuf, or a failed future if there was an
    // error. Failed futures always carry exceptions inheriting from
    // serde::protobuf::rpc::base_exception.
    ss::future<iobuf> send(
      model::node_id target,
      serde::pb::rpc::context ctx,
      iobuf payload) noexcept;

    model::node_id _self;
    ss::sharded<rpc::connection_cache>* _conn_cache;
    std::function<std::vector<model::node_id>()> _all_node_ids_fn;
};

} // namespace admin::proxy
