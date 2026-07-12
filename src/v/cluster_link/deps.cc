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

#include "cluster_link/deps.h"

#include "cluster/members_table.h"
#include "cluster/security_frontend.h"
#include "cluster_link/utils.h"
#include "features/feature_table.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/serde.h"
#include "security/role_store.h"

namespace cluster_link {

namespace {
class security_impl : public security_service {
public:
    security_impl(
      ss::sharded<cluster::security_frontend>* security_fe,
      ss::sharded<security::role_store>* role_store,
      ss::sharded<features::feature_table>* features)
      : _security_fe(security_fe)
      , _role_store(role_store)
      , _features(features) {}

    ss::future<chunked_vector<cluster::errc>> create_acls(
      chunked_vector<security::acl_binding> bindings,
      ::model::timeout_clock::duration timeout) final {
        return _security_fe->local().create_acls(std::move(bindings), timeout);
    }

    ss::future<std::error_code> create_role(
      security::role_name name,
      security::role role,
      ::model::timeout_clock::duration timeout) final {
        return _security_fe->local().create_role(
          std::move(name),
          std::move(role),
          ::model::timeout_clock::now() + timeout);
    }

    ss::future<std::error_code> update_role(
      security::role_name name,
      security::role role,
      ::model::timeout_clock::duration timeout) final {
        return _security_fe->local().update_role(
          std::move(name),
          std::move(role),
          ::model::timeout_clock::now() + timeout);
    }

    ss::future<std::error_code> delete_role(
      security::role_name name,
      ::model::timeout_clock::duration timeout) final {
        return _security_fe->local().delete_role(
          std::move(name), ::model::timeout_clock::now() + timeout);
    }

    bool rbac_active() const final {
        return _features->local().is_active(
          features::feature::role_based_access_control);
    }

    chunked_vector<security::role_with_members> read_shadow_roles(
      const std::function<bool(const security::role_name&)>& pred) const final {
        return _role_store->local().roles_with_members(pred);
    }

private:
    ss::sharded<cluster::security_frontend>* _security_fe;
    ss::sharded<security::role_store>* _role_store;
    ss::sharded<features::feature_table>* _features;
};

class kafka_rpc_client_impl : public kafka_rpc_client_service {
public:
    explicit kafka_rpc_client_impl(
      ss::sharded<kafka::data::rpc::client>* client)
      : _client(client) {}

    ss::future<result<kafka::data::rpc::partition_offsets_map, cluster::errc>>
    get_partition_offsets(
      chunked_vector<kafka::data::rpc::topic_partitions> tps) final {
        return _client->local().get_partition_offsets(std::move(tps));
    }

private:
    ss::sharded<kafka::data::rpc::client>* _client;
};

class members_table_provider_impl : public members_table_provider {
public:
    explicit members_table_provider_impl(
      ss::sharded<cluster::members_table>* members_table)
      : _members_table(members_table) {}

    size_t node_count() const final {
        return _members_table->local().node_count();
    }

private:
    ss::sharded<cluster::members_table>* _members_table;
};
} // namespace

std::unique_ptr<security_service> security_service::make_default(
  ss::sharded<cluster::security_frontend>* security_fe,
  ss::sharded<security::role_store>* role_store,
  ss::sharded<features::feature_table>* features) {
    return std::make_unique<security_impl>(security_fe, role_store, features);
}

std::unique_ptr<kafka::client::cluster>
cluster_factory::create_cluster(const model::metadata& md) {
    return std::make_unique<kafka::client::cluster>(
      metadata_to_kafka_config(md));
}

std::unique_ptr<kafka_rpc_client_service>
kafka_rpc_client_service::make_default(
  ss::sharded<kafka::data::rpc::client>* client) {
    return std::make_unique<kafka_rpc_client_impl>(client);
}

std::unique_ptr<members_table_provider> members_table_provider::make_default(
  ss::sharded<cluster::members_table>* members_table) {
    return std::make_unique<members_table_provider_impl>(members_table);
}
} // namespace cluster_link
