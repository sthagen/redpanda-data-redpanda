/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/app.h"

#include "cloud_topics/cluster_services.h"
#include "cloud_topics/data_plane_impl.h"
#include "cluster/cluster_epoch_service.h"
#include "cluster/controller.h"
#include "ssx/sharded_service_container.h"

#include <seastar/core/coroutine.hh>

namespace experimental::cloud_topics {

namespace {
class real_cluster_services
  : public experimental::cloud_topics::cluster_services {
public:
    explicit real_cluster_services(
      ss::sharded<cluster::cluster_epoch_service<>>* epoch_generator)
      : _epoch_service(epoch_generator) {}

    seastar::future<experimental::cloud_topics::cluster_epoch>
    current_epoch(seastar::abort_source* as) override {
        std::expected<int64_t, std::error_code> epoch
          = co_await _epoch_service->local().get_cached_epoch(as);
        if (!epoch) {
            throw std::system_error(epoch.error());
        }
        co_return experimental::cloud_topics::cluster_epoch(epoch.value());
    }

private:
    ss::sharded<cluster::cluster_epoch_service<>>* _epoch_service;
};
} // namespace

app::app()
  : ssx::sharded_service_container("cloud_topics::app") {}

ss::future<> app::construct(
  model::node_id self,
  cluster::controller* controller,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::partition_leaders_table>* leaders_table,
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cloud_io::remote>* remote,
  ss::sharded<cloud_storage::cache>* cloud_cache,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  ss::sharded<rpc::connection_cache>* connection_cache,
  cloud_storage_clients::bucket_name bucket,
  ss::sharded<storage::api>* storage) {
    co_await construct_service(
      state,
      ss::sharded_parameter([remote, cloud_cache, bucket, storage, controller] {
          return experimental::cloud_topics::make_data_plane(
            remote,
            cloud_cache,
            bucket,
            storage,
            std::make_unique<real_cluster_services>(
              &controller->get_cluster_epoch_generator()));
      }));
    co_await construct_service(
      reconciler,
      partition_mgr,
      remote,
      ss::sharded_parameter([this] { return state.local().get_data_plane(); }),
      bucket);
    co_await construct_service(domain_supervisor, controller);
    co_await construct_service(
      l1_metastore_fe,
      self,
      metadata_cache,
      leaders_table,
      shard_table,
      connection_cache,
      &domain_supervisor);
}

ss::future<> app::start() {
    co_await state.invoke_on_all([](auto& s) { return s.start(); });
    co_await reconciler.invoke_on_all([](auto& r) { return r.start(); });
    co_await domain_supervisor.invoke_on_all(
      [](auto& ds) { return ds.start(); });
}

ss::future<> app::stop() {
    ssx::sharded_service_container::shutdown();
    co_return;
}

ss::sharded<l1::frontend>* app::get_sharded_l1_metastore_fe() {
    return &l1_metastore_fe;
}

ss::sharded<state_accessors>* app::get_state() { return &state; }

} // namespace experimental::cloud_topics
