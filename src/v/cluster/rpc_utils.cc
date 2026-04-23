// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rpc_utils.h"

#include "config/node_config.h"
#include "model/fips_config.h"
#include "model/metadata.h"

#include <seastar/core/smp.hh>

#include <filesystem>

namespace cluster {

ss::future<> remove_broker_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id id) {
    return clients.local().remove_broker_client(self, id);
}

ss::future<> update_broker_client(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& clients,
  model::node_id node,
  net::unresolved_address addr,
  config::tls_config tls_config) {
    return clients.local().update_broker_client(
      self, node, std::move(addr), std::move(tls_config));
}

model::broker make_self_broker(const config::node_config& node_cfg) {
    auto kafka_addr = node_cfg.advertised_kafka_api();
    auto rpc_addr = node_cfg.advertised_rpc_api();

    // Calculate memory size
    const auto shard_mem = ss::memory::stats();
    uint64_t total_mem = shard_mem.total_memory() * ss::smp::count;
    // If memory is <1GB, we'll return zero.  That case is already
    // handled when reading this field (e.g. in
    // `partition_allocator::check_cluster_limits`) because earlier redpanda
    // versions always returned zero here.
    uint32_t total_mem_gb = total_mem >> 30;

    // Calculate disk size
    auto space_info = std::filesystem::space(
      config::node().data_directory().path);
    // As for memory, if disk_gb is zero this is handled like a legacy broker.
    uint32_t disk_gb = space_info.capacity >> 30;

    // If this node hasn't been configured with a node ID, use -1 to indicate
    // that we don't yet know it yet. This shouldn't be used during the normal
    // operation of a broker, and instead should be used to indicate a broker
    // that needs to be assigned a node ID when it first starts up.
    model::node_id node_id = node_cfg.node_id() == std::nullopt
                               ? model::unassigned_node_id
                               : *node_cfg.node_id();
    return model::broker(
      node_id,
      kafka_addr,
      rpc_addr,
      node_cfg.rack,
      model::broker_properties{
        // TODO: populate or remote etc_props, mount_paths
        .cores = ss::smp::count,
        .available_memory_gb = total_mem_gb,
        .available_disk_gb = disk_gb,
        .available_memory_bytes = total_mem,
        .in_fips_mode = model::from_config(node_cfg.fips_mode())});
}

} // namespace cluster
