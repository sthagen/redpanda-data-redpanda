/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/client_quota_translator.h"

#include "cluster/client_quota_store.h"
#include "config/configuration.h"

namespace kafka {

using cluster::client_quota::entity_key;
using cluster::client_quota::entity_value;

std::ostream& operator<<(std::ostream& os, const client_quota_limits& l) {
    fmt::print(
      os,
      "limits{{produce_limit: {}, fetch_limit: {}, "
      "partition_mutation_limit: {}}}",
      l.produce_limit,
      l.fetch_limit,
      l.partition_mutation_limit);
    return os;
}

client_quota_translator::client_quota_translator(
  ss::sharded<cluster::client_quota::store>& quota_store)
  : _quota_store(quota_store)
  , _default_target_produce_tp_rate(
      config::shard_local_cfg().target_quota_byte_rate.bind())
  , _default_target_fetch_tp_rate(
      config::shard_local_cfg().target_fetch_quota_byte_rate.bind())
  , _target_partition_mutation_quota(
      config::shard_local_cfg().kafka_admin_topic_api_rate.bind())
  , _target_produce_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_byte_rate_quota.bind())
  , _target_fetch_tp_rate_per_client_group(
      config::shard_local_cfg()
        .kafka_client_group_fetch_byte_rate_quota.bind()) {}

std::optional<uint64_t> client_quota_translator::get_client_quota_value(
  const tracker_key& quota_id, client_quota_type qt) const {
    const auto accessor = [qt](const cluster::client_quota::entity_value& ev) {
        switch (qt) {
        case client_quota_type::produce_quota:
            return ev.producer_byte_rate;
        case client_quota_type::fetch_quota:
            return ev.consumer_byte_rate;
        case client_quota_type::partition_mutation_quota:
            return ev.controller_mutation_rate;
        }
    };
    return ss::visit(
      quota_id,
      [this, qt, &accessor](const k_client_id& k) -> std::optional<uint64_t> {
          auto exact_match_key = entity_key{
            .parts = {entity_key::part{
              .part = entity_key::part::client_id_match{.value = k},
            }},
          };
          auto exact_match_quota = _quota_store.local().get_quota(
            exact_match_key);
          if (exact_match_quota && accessor(*exact_match_quota)) {
              return accessor(*exact_match_quota);
          }

          const static auto default_client_key = entity_key{
            .parts = {entity_key::part{
              .part = entity_key::part::client_id_default_match{},
            }},
          };
          auto default_quota = _quota_store.local().get_quota(
            default_client_key);
          if (default_quota && accessor(*default_quota)) {
              return accessor(*default_quota);
          }

          return get_default_config(qt);
      },
      [this, qt, &accessor](const k_group_name& k) -> std::optional<uint64_t> {
          const auto& group_quota_config = get_quota_config(qt);
          auto group_key = entity_key{
            .parts = {entity_key::part{
              .part = entity_key::part::client_id_prefix_match{.value = k},
            }},
          };
          auto group_quota = _quota_store.local().get_quota(group_key);
          if (group_quota && accessor(*group_quota)) {
              return accessor(*group_quota);
          }

          auto group = group_quota_config.find(k);
          if (group != group_quota_config.end()) {
              return group->second.quota;
          }

          return {};
      });
}

// If client is part of some group then client quota ID is a group
// else client quota ID is client_id
tracker_key client_quota_translator::find_quota_key(
  const client_quota_request_ctx& ctx) const {
    auto qt = ctx.q_type;
    const auto& client_id = ctx.client_id;
    const auto& group_quota = get_quota_config(qt);
    const auto& quota_store = _quota_store.local();

    const auto checker = [qt](const entity_value val) {
        switch (qt) {
        case kafka::client_quota_type::produce_quota:
            return val.producer_byte_rate.has_value();
        case kafka::client_quota_type::fetch_quota:
            return val.consumer_byte_rate.has_value();
        case kafka::client_quota_type::partition_mutation_quota:
            return val.controller_mutation_rate.has_value();
        }
    };

    if (!client_id) {
        // requests without a client id are grouped into an anonymous group that
        // shares a default quota. the anonymous group is keyed on empty string.
        return tracker_key{std::in_place_type<k_client_id>, ""};
    }

    // Exact match quotas
    auto exact_match_key = entity_key{
      .parts = {entity_key::part{
        .part
        = entity_key::part::client_id_match{.value = ss::sstring{*client_id}},
      }},
    };
    auto exact_match_quota = quota_store.get_quota(exact_match_key);
    if (exact_match_quota && checker(*exact_match_quota)) {
        return tracker_key{std::in_place_type<k_client_id>, *client_id};
    }

    // Group quotas configured through the Kafka API
    auto group_quotas = quota_store.range(
      cluster::client_quota::store::prefix_group_filter(*client_id));
    for (auto& [gk, gv] : group_quotas) {
        if (checker(gv)) {
            for (auto& part : gk.parts) {
                using client_id_prefix_match
                  = entity_key::part::client_id_prefix_match;

                if (std::holds_alternative<client_id_prefix_match>(part.part)) {
                    auto& prefix_key_part = get<client_id_prefix_match>(
                      part.part);
                    return tracker_key{
                      std::in_place_type<k_group_name>, prefix_key_part.value};
                }
            }
        }
    }

    // Group quotas configured through cluster configs
    for (const auto& group_and_limit : group_quota) {
        if (client_id->starts_with(
              std::string_view(group_and_limit.second.clients_prefix))) {
            return tracker_key{
              std::in_place_type<k_group_name>, group_and_limit.first};
        }
    }

    // Default quotas configured through either the Kafka API or cluster configs
    return tracker_key{std::in_place_type<k_client_id>, *client_id};
}

std::pair<tracker_key, client_quota_limits>
client_quota_translator::find_quota(const client_quota_request_ctx& ctx) const {
    auto key = find_quota_key(ctx);
    auto value = find_quota_value(key);
    return {std::move(key), value};
}

client_quota_limits
client_quota_translator::find_quota_value(const tracker_key& key) const {
    return client_quota_limits{
      .produce_limit = get_client_quota_value(
        key, client_quota_type::produce_quota),
      .fetch_limit = get_client_quota_value(
        key, client_quota_type::fetch_quota),
      .partition_mutation_limit = get_client_quota_value(
        key, client_quota_type::partition_mutation_quota),
    };
}

void client_quota_translator::watch(on_change_fn&& fn) {
    auto watcher = [fn = std::move(fn)]() { fn(); };
    _target_produce_tp_rate_per_client_group.watch(watcher);
    _target_fetch_tp_rate_per_client_group.watch(watcher);
    _target_partition_mutation_quota.watch(watcher);
    _default_target_produce_tp_rate.watch(watcher);
    _default_target_fetch_tp_rate.watch(watcher);
}

const client_quota_translator::quota_config&
client_quota_translator::get_quota_config(client_quota_type qt) const {
    static const quota_config empty;
    switch (qt) {
    case kafka::client_quota_type::produce_quota:
        return _target_produce_tp_rate_per_client_group();
    case kafka::client_quota_type::fetch_quota:
        return _target_fetch_tp_rate_per_client_group();
    case kafka::client_quota_type::partition_mutation_quota:
        return empty;
    }
}

std::optional<uint64_t>
client_quota_translator::get_default_config(client_quota_type qt) const {
    switch (qt) {
    case kafka::client_quota_type::produce_quota:
        return _default_target_produce_tp_rate();
    case kafka::client_quota_type::fetch_quota:
        return _default_target_fetch_tp_rate();
    case kafka::client_quota_type::partition_mutation_quota:
        return _target_partition_mutation_quota();
    }
}

} // namespace kafka
