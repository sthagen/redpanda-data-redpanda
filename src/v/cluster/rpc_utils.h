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
#include "config/node_config.h"
#include "config/tls_config.h"
#include "model/metadata.h"
#include "rpc/connection_cache.h"
#include "rpc/rpc_utils.h"
#include "rpc/types.h"

#include <seastar/core/sharded.hh>

#include <utility>

namespace cluster {

ss::future<> update_broker_client(
  model::node_id,
  ss::sharded<rpc::connection_cache>&,
  model::node_id node,
  net::unresolved_address addr,
  config::tls_config);

ss::future<> remove_broker_client(
  model::node_id, ss::sharded<rpc::connection_cache>&, model::node_id);

template<typename Proto, typename Func>
requires requires(Func&& f, Proto c) { f(c); }
auto with_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& cache,
  model::node_id id,
  net::unresolved_address addr,
  config::tls_config tls_config,
  rpc::clock_type::duration connection_timeout,
  Func&& f) {
    return update_broker_client(
             self, cache, id, std::move(addr), std::move(tls_config))
      .then([id,
             self,
             &cache,
             f = std::forward<Func>(f),
             connection_timeout]() mutable {
          return cache.local().with_node_client<Proto, Func>(
            self,
            ss::this_shard_id(),
            id,
            connection_timeout,
            std::forward<Func>(f));
      });
}

/// Creates current broker instance using its configuration.
model::broker make_self_broker(const config::node_config& node_cfg);

template<typename Proto, typename Func>
requires requires(Func&& f, Proto c) { f(c); }
auto do_with_client_one_shot(
  net::unresolved_address addr,
  config::tls_config tls_config,
  rpc::clock_type::duration connection_timeout,
  rpc::transport_version v,
  Func&& f) {
    return rpc::maybe_build_reloadable_certificate_credentials(
             std::move(tls_config))
      .then([v,
             f = std::forward<Func>(f),
             connection_timeout,
             addr = std::move(addr)](
              ss::shared_ptr<ss::tls::certificate_credentials>&& cert) mutable {
          auto transport = ss::make_lw_shared<rpc::transport>(
            rpc::transport_configuration{
              .server_addr = std::move(addr),
              .credentials = std::move(cert),
              .disable_metrics = net::metrics_disabled(true),
              .version = v});

          return transport->connect(connection_timeout)
            .then([transport, f = std::forward<Func>(f)]() mutable {
                return ss::futurize_invoke(
                  std::forward<Func>(f), Proto(transport));
            })
            .finally([transport] {
                transport->shutdown();
                return transport->stop().finally([transport] {});
            });
      });
}

} // namespace cluster
