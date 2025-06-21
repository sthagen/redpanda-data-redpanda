/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/frontend.h"

#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "rpc/connection_cache.h"

namespace cluster::cluster_link {

using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::name_t;

using mutation_result = frontend::mutation_result;

namespace {
cluster::errc map_errc(std::error_code ec) {
    if (ec == cluster::errc::success) {
        return cluster::errc::success;
    }
    if (ec.category() == raft::error_category()) {
        switch (raft::errc(ec.value())) {
        case raft::errc::timeout:
            return cluster::errc::timeout;
        case raft::errc::not_leader:
            return cluster::errc::not_leader_controller;
        default:
            return cluster::errc::replication_error;
        }
    }
    if (ec.category() == rpc::error_category()) {
        switch (rpc::errc(ec.value())) {
        case rpc::errc::client_request_timeout:
            return cluster::errc::timeout;
        default:
            return cluster::errc::replication_error;
        }
    }
    if (ec.category() == cluster::error_category()) {
        return cluster::errc(ec.value());
    }
    return cluster::errc::replication_error;
}
} // namespace

frontend::frontend(
  model::node_id self,
  cluster::partition_leaders_table* leaders,
  table* table,
  cluster::controller_stm* controller,
  rpc::connection_cache* connections,
  features::feature_table* features,
  ss::abort_source* as)
  : _self(self)
  , _leaders(leaders)
  , _connections(connections)
  , _table(table)
  , _as(as)
  , _controller(controller)
  , _features(features) {}

ss::future<mutation_result> frontend::upsert_cluster_link(
  ::cluster_link::model::metadata meta,
  model::timeout_clock::time_point timeout) {
    if (!cluster_link_active(true)) {
        co_return mutation_result{.ec = cluster::errc::feature_disabled};
    }
    cluster_link_cmd c{cluster::cluster_link_upsert_cmd{0, std::move(meta)}};
    co_return co_await do_mutation(std::move(c), timeout);
}

ss::future<mutation_result> frontend::remove_cluster_link(
  ::cluster_link::model::name_t name,
  model::timeout_clock::time_point timeout) {
    if (!cluster_link_active(false)) {
        co_return mutation_result{.ec = cluster::errc::feature_disabled};
    }
    cluster_link_cmd c{cluster::cluster_link_remove_cmd(std::move(name), 0)};
    co_return co_await do_mutation(std::move(c), timeout);
}

bool frontend::cluster_link_active(bool check_license) const {
    return _features->is_active(features::feature::cluster_linking_dr)
           && !(check_license && _features->should_sanction());
}

frontend::notification_id
frontend::register_for_updates(notification_callback cb) {
    return _table->register_for_updates(std::move(cb));
}

void frontend::unregister_for_updates(notification_id id) {
    _table->unregister_for_updates(id);
}

std::optional<std::reference_wrapper<const metadata>>
frontend::find_link_by_id(id_t id) const {
    return _table->find_link_by_id(id);
}

std::optional<std::reference_wrapper<const metadata>>
frontend::find_link_by_name(const name_t& name) const {
    return _table->find_link_by_name(name);
}

chunked_vector<id_t> frontend::get_all_link_ids() const {
    return _table->get_all_link_ids();
}

ss::future<mutation_result> frontend::do_mutation(
  cluster_link_cmd cmd, model::timeout_clock::time_point timeout) {
    auto cluster_leader = _leaders->get_leader(model::controller_ntp);
    if (!cluster_leader) {
        co_return mutation_result{.ec = cluster::errc::not_leader_controller};
    }
    if (*cluster_leader != _self) {
        co_return co_await dispatch_mutation_to_remote(
          *cluster_leader,
          std::move(cmd),
          timeout - model::timeout_clock::now());
    }

    co_return co_await container().invoke_on(
      cluster::controller_stm_shard,
      [cmd = std::move(cmd), timeout](auto& service) mutable {
          return service.do_local_mutation(std::move(cmd), timeout);
      });
}

ss::future<mutation_result> frontend::dispatch_mutation_to_remote(
  model::node_id cluster_leader,
  cluster_link_cmd cmd,
  model::timeout_clock::duration timeout) {
    return _connections
      ->with_node_client<cluster::controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        cluster_leader,
        timeout,
        [timeout, cmd = std::move(cmd)](
          cluster::controller_client_protocol client) mutable {
            return ss::visit(
              std::move(cmd),
              [client, timeout](cluster::cluster_link_upsert_cmd cmd) mutable {
                  return client
                    .upsert_cluster_link(
                      cluster::upsert_cluster_link_request{
                        .metadata = std::move(cmd.value), .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(
                      &rpc::get_ctx_data<cluster::upsert_cluster_link_response>)
                    .then([](result<cluster::upsert_cluster_link_response> r) {
                        if (r.has_error()) {
                            return result<mutation_result>(r.error());
                        }
                        return result<mutation_result>(
                          mutation_result{.ec = r.value().ec});
                    });
              },
              [client, timeout](cluster::cluster_link_remove_cmd cmd) mutable {
                  return client
                    .remove_cluster_link(
                      cluster::remove_cluster_link_request{
                        .name = std::move(cmd.key), .timeout = timeout},
                      rpc::client_opts(timeout))
                    .then(
                      &rpc::get_ctx_data<cluster::remove_cluster_link_response>)
                    .then([](result<cluster::remove_cluster_link_response> r) {
                        if (r.has_error()) {
                            return result<mutation_result>(r.error());
                        }
                        return result<mutation_result>(
                          mutation_result{.ec = r.value().ec});
                    });
              });
        })
      .then([](result<mutation_result> r) {
          if (r.has_error()) {
              return mutation_result{.ec = map_errc(r.error())};
          }
          return r.value();
      });
}

ss::future<mutation_result> frontend::do_local_mutation(
  cluster_link_cmd cmd, model::timeout_clock::time_point timeout) {
    auto u = co_await _mu.get_units();
    auto result = co_await _controller->insert_linearizable_barrier(timeout);
    if (!result) {
        co_return mutation_result{.ec = cluster::errc::not_leader_controller};
    }
    auto ec = validate_mutation(cmd);
    if (ec != cluster::errc::success) {
        co_return mutation_result{.ec = ec};
    }
    auto ok = std::visit(
      [this](const auto& cmd) {
          using T = std::decay_t<decltype(cmd)>;
          return _controller->throttle<T>();
      },
      cmd);
    if (!ok) {
        co_return mutation_result{
          .ec = cluster::errc::throttling_quota_exceeded};
    }

    auto b = std::visit(
      [](auto cmd) { return serde_serialize_cmd(std::move(cmd)); },
      std::move(cmd));
    auto err_code = co_await _controller->replicate_and_wait(
      std::move(b), timeout, *_as);
    co_return mutation_result{.ec = map_errc(err_code)};
}

cluster::errc frontend::validate_mutation(const cluster_link_cmd& cmd) const {
    // Initially for DR, we will only support a single cluster link at a time.
    static constexpr size_t max_links = 1;
    validator v{_table, max_links};
    return v.validate_mutation(cmd);
}

frontend::validator::validator(table* table, size_t max_links)
  : _table(table)
  , _max_links(max_links) {}

cluster::errc
frontend::validator::validate_mutation(const cluster_link_cmd& cmd) const {
    return ss::visit(
      cmd,
      [this](const cluster::cluster_link_upsert_cmd& cmd) {
          auto existing = _table->find_link_by_name(cmd.value.name);
          if (existing.has_value()) {
              // upsert
              const auto& meta = existing->get();
              if (meta.uuid != cmd.value.uuid) {
                  // If the UUIDs do not match, it means we are trying to
                  // update an existing link with a different UUID.
                  vlog(
                    cluster::clusterlog.info,
                    "Attempting to upsert a panda link with name {} with a "
                    "different UUID ({}) than the existing one ({})",
                    cmd.value.name,
                    cmd.value.uuid,
                    meta.uuid);
                  return cluster::errc::cluster_link_invalid_update;
              }
              if (cmd.value.connection.bootstrap_servers.empty()) {
                  vlog(
                    cluster::clusterlog.info,
                    "Attempting to update a panda link without bootstrap "
                    "servers");
                  return cluster::errc::cluster_link_invalid_update;
              }
              return cluster::errc::success;
          }
          // New item!
          if (cmd.value.name().empty()) {
              vlog(
                cluster::clusterlog.info,
                "Attempting to create a panda link without a name");
              return cluster::errc::cluster_link_invalid_create;
          }
          constexpr static size_t max_name_size = 128;
          if (cmd.value.name().size() > max_name_size) {
              vlog(
                cluster::clusterlog.info,
                "Attempting to create a panda link with too large of a name "
                "{} > {}",
                cmd.value.name().size(),
                max_name_size);
              return cluster::errc::cluster_link_invalid_create;
          }
          if (!std::ranges::all_of(cmd.value.name(), [](char c) {
                  return std::isalnum(c) || c == '.' || c == '-' || c == '_';
              })) {
              vlog(
                cluster::clusterlog.info,
                "Attempting to create a panda link with a name containing "
                "invalid characters");
              return cluster::errc::cluster_link_invalid_create;
          }
          if (cmd.value.connection.bootstrap_servers.empty()) {
              vlog(
                cluster::clusterlog.info,
                "Attempting to create a panda link without bootstrap servers");
              return cluster::errc::cluster_link_invalid_create;
          }
          if (_table->size() >= _max_links) {
              vlog(
                cluster::clusterlog.info,
                "Attempting to create a panda link when the maximum number of "
                "links ({}) is already reached",
                _max_links);
              return cluster::errc::cluster_link_limit_exceeded;
          }

          return cluster::errc::success;
      },
      [this](const cluster::cluster_link_remove_cmd& cmd) {
          auto meta = _table->find_link_by_name(cmd.key);
          if (!meta.has_value()) {
              return cluster::errc::cluster_link_does_not_exist;
          }
          return cluster::errc::success;
      });
}
} // namespace cluster::cluster_link
