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
#include "kafka/client/test/cluster_mock.h"

namespace kafka::client {

broker_mock::broker_mock(
  cluster_mock* cluster_mock, model::node_id id, net::unresolved_address addr)
  : _cluster_mock(cluster_mock)
  , _id(id)
  , _addr(std::move(addr)) {}

ss::future<response_t> broker_mock::dispatch(
  request_t request,
  api_version version,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return _cluster_mock->handle(
      _id,
      std::move(request),
      version,
      as); // Assuming handle is a method in cluster_mock
}

ss::future<std::optional<api_version_range>>
broker_mock::get_supported_versions(
  api_key key, std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (_supported_versions.empty()) {
        auto resp = co_await dispatch(
          api_versions_request{},
          api_versions_request::api_type::max_valid,
          as);

        auto api_versions_resp = std::get<api_versions_response>(
          std::move(resp));
        for (const auto& resp_key : api_versions_resp.data.api_keys) {
            _supported_versions[api_key(resp_key.api_key)] = api_version_range{
              .min = api_version(resp_key.min_version),
              .max = api_version(resp_key.max_version),
            };
        }
    }
    auto it = _supported_versions.find(key);
    if (it == _supported_versions.end()) {
        co_return std::nullopt;
    }
    co_return it->second;
}

void cluster_mock::register_default_handlers() {
    register_handler(
      metadata_api::key,
      [this](model::node_id id, request_t req, api_version version) {
          return handle_metadata_request(id, std::move(req), version);
      });

    register_handler(
      api_versions_api::key,
      [this](model::node_id id, request_t req, api_version version) {
          return handle_api_versions_request(id, std::move(req), version);
      });
}

void cluster_mock::register_handler(api_key key, mock_handler handler) {
    auto& h = _handlers[key];
    h.default_handler = std::move(handler);
}
void cluster_mock::register_broker_handler(
  model::node_id id, api_key key, mock_handler handler) {
    auto& h = _handlers[key];
    h.per_node_handlers[id] = std::move(handler);
}

ss::future<response_t> cluster_mock::handle_metadata_request(
  model::node_id, request_t req, api_version) {
    auto md_req = std::get<metadata_request>(std::move(req));
    metadata_response_data r_data;
    for (auto& b : _brokers) {
        r_data.brokers.push_back(metadata_response::broker{
          .node_id = b.second.id,
          .host = b.second.address.host(),
          .port = b.second.address.port(),
          .rack = b.second.rack,
        });
    }

    for (const auto& [topic, md] : _topics) {
        metadata_response::topic md_topic;
        // TODO - Update when supporting topic IDs on RP
        md_topic.name = topic;

        md_topic.topic_authorized_operations
          = md_req.data.include_topic_authorized_operations
              ? md.authorized_operations
              : kafka::topic_authorized_operations_not_set;
        md_topic.partitions.reserve(md.partitions.size());
        for (auto& [part_id, part_meta] : md.partitions) {
            md_topic.partitions.push_back(metadata_response::partition{
              .partition_index = part_id,
              .leader_id = part_meta.leader,
              .leader_epoch = part_meta.leader_epoch,
              .replica_nodes = part_meta.replicas});
        }
        r_data.topics.push_back(std::move(md_topic));
    }

    r_data.controller_id = _brokers.begin()->first;

    co_return metadata_response{.data = std::move(r_data)};
}

namespace {
api_versions_response
make_api_versions_response(const supported_versions& versions) {
    api_versions_response_data r_data;
    for (const auto& [key, range] : versions) {
        r_data.api_keys.push_back(api_versions_response_key{
          .api_key = key, .min_version = range.min, .max_version = range.max});
    }
    return api_versions_response{.data = std::move(r_data)};
}
} // namespace

ss::future<response_t> cluster_mock::handle_api_versions_request(
  model::node_id node_id, request_t, api_version) {
    auto it = _broker_api_versions.find(node_id);
    if (it == _broker_api_versions.end()) {
        co_return make_api_versions_response(default_supported_versions);
    }
    co_return make_api_versions_response(it->second);
}

template<typename ReqT, typename Ret>
requires(KafkaApi<typename ReqT::api_type>)
ss::future<Ret> cluster_mock::do_handle(
  model::node_id node_id,
  request_t req,
  api_version version,
  std::optional<std::reference_wrapper<ss::abort_source>>) {
    using api_t = typename ReqT::api_type;
    _logger.info(
      "handling request node: {}, api: {}, request: {}",
      node_id,
      api_t::name,
      req);

    auto it = _handlers.find(api_t::key);
    if (it == _handlers.end()) {
        throw std::runtime_error(
          fmt::format("No handler registered for API key: {}", api_t::key));
    }
    auto node_handler_it = it->second.per_node_handlers.find(node_id);
    if (node_handler_it != it->second.per_node_handlers.end()) {
        // If a specific handler for the node is registered, use it
        return node_handler_it->second(node_id, std::move(req), version)
          .then([](response_t resp) { return std::get<Ret>(std::move(resp)); });
    }
    return it->second.default_handler(node_id, std::move(req), version)
      .then([](response_t resp) { return std::get<Ret>(std::move(resp)); });
}

ss::future<response_t> cluster_mock::handle(
  model::node_id node_id,
  request_t req,
  api_version version,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return ss::visit(std::move(req), [this, node_id, version, &as](auto r) {
        return do_handle<decltype(r)>(node_id, std::move(r), version, as)
          .then([](auto resp) { return response_t{std::move(resp)}; });
    });
}

void cluster_mock::add_topic(
  model::topic topic_name,
  size_t partition_count,
  size_t replication_factor,
  kafka::topic_authorized_operations authorized_operations) {
    if (_topics.contains(topic_name)) {
        // Topic already exists, do not overwrite
        throw std::invalid_argument(
          fmt::format("Topic {} already exists", topic_name));
    }
    if (replication_factor > _brokers.size()) {
        throw std::invalid_argument(fmt::format(
          "Replication factor {} exceeds available brokers",
          replication_factor));
    }

    auto cluster_nodes = get_broker_ids();
    std::ranges::sort(cluster_nodes);

    topic_metadata md;
    md.authorized_operations = authorized_operations;

    for (auto p_id : std::views::iota(size_t(0), partition_count)) {
        partition_metadata p_md{
          .id = model::partition_id(p_id), .leader = model::node_id(-1)};

        std::copy_n(
          cluster_nodes.begin(),
          replication_factor,
          std::back_inserter(p_md.replicas));
        std::ranges::rotate(cluster_nodes, cluster_nodes.begin() + 1);
        p_md.leader = p_md.replicas[0];
        p_md.leader_epoch = kafka::invalid_leader_epoch;
        md.partitions.emplace(model::partition_id(p_id), std::move(p_md));
    }

    _topics.emplace(topic_name, std::move(md));
}

cluster_mock::cluster_mock()
  : _logger(kclog, "cluster-mock") {
    default_supported_versions[metadata_api::key] = {
      .min = kafka::metadata_api::min_valid,
      .max = kafka::metadata_api::max_valid};
    default_supported_versions[api_versions_api::key] = {
      .min = kafka::api_versions_api::min_valid,
      .max = kafka::api_versions_api::max_valid};
}
} // namespace kafka::client
