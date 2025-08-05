/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#include "cluster_link/source_topic_syncer.h"

#include "cluster_link/link.h"
#include "cluster_link/model/filter_utils.h"
#include "cluster_link/model/types.h"
#include "kafka/server/handlers/topics/types.h"

#include <fmt/ranges.h>

namespace cluster_link {

namespace {
const absl::flat_hash_set<ss::sstring> required_topic_properties{
  ss::sstring{kafka::topic_property_max_message_bytes},
  ss::sstring{kafka::topic_property_cleanup_policy},
  ss::sstring{kafka::topic_property_timestamp_type},
};

bool has_required_permissions(
  kafka::topic_authorized_operations permissions_to_check,
  kafka::topic_authorized_operations required_permissions) {
    return (permissions_to_check & required_permissions)
           == required_permissions;
}

} // namespace

source_topic_syncer::source_topic_syncer(
  link* link, const model::metadata& config)
  : task(
      link,
      config.state.topic_metadata_mirroring_cfg.task_interval,
      source_topic_syncer::task_name)
  , _config(config.state.topic_metadata_mirroring_cfg.copy()) {}

task::is_locked_to_controller
source_topic_syncer::locked_to_controller() const noexcept {
    return is_locked_to_controller::yes;
}

void source_topic_syncer::update_config(const model::metadata& config) {
    _config = config.state.topic_metadata_mirroring_cfg.copy();
    set_run_interval(config.state.topic_metadata_mirroring_cfg.task_interval);
}

ss::future<> source_topic_syncer::run_impl() {
    /// The auto topic sensor task is responsible for identifying topics on the
    /// source cluster that are candidates to be mirrored.  To determine if a
    /// topic is a candidate the task will:
    /// 1. Grab the metadata from the source cluster
    /// 2. Check to see if the topic already exists or if it is already being
    /// mirrored
    /// 3. Check to see if there are inclusive filters for that topic
    /// 4. Validate that the topic's permissions are sufficient for mirroring
    /// Once that selection criteria is set, then the task will fetch the
    /// configs for that topic and then add that topic to the table or mirror
    /// topics.  A seperate task will then be responsible for reconciling the
    /// contents of that table with the destination cluster
    vlog(logger().trace, "Running auto topic sensor task");

    auto& cluster = get_link()->get_cluster_connection();

    try {
        co_await cluster.request_metadata_update();
    } catch (const std::exception& e) {
        auto msg = ssx::sformat("Failed to update metadata: {}", e.what());
        vlog(logger().warn, "{}", msg);
        std::ignore = change_state(model::task_state::link_unavailable, msg);
        co_return;
    }

    auto controller_id = cluster.get_controller_id();
    if (!controller_id) {
        auto msg = ssx::sformat(
          "Failed to get controller id for link {}",
          get_link()->get_config().name);
        vlog(logger().warn, "{}", msg);
        std::ignore = change_state(model::task_state::link_unavailable, msg);
        co_return;
    }

    kafka::api_version describe_configs_version;
    try {
        auto supported_api_versions = co_await cluster.supported_api_versions(
          kafka::describe_configs_api::key);
        if (!supported_api_versions.has_value()) {
            auto msg = ssx::sformat(
              "Failed to get supported API version for describe_configs");
            vlog(logger().warn, "{}", msg);
            std::ignore = change_state(
              model::task_state::link_unavailable, msg);
            co_return;
        }
        describe_configs_version = std::min(
          supported_api_versions.value().max,
          kafka::describe_configs_api::max_valid);
        vlog(
          logger().debug,
          "Using describe_configs version: {}",
          describe_configs_version);
    } catch (const std::exception& e) {
        auto msg = ssx::sformat(
          "Failed to get supported API version for describe_configs: {}",
          e.what());
        vlog(logger().warn, "{}", msg);
        std::ignore = change_state(model::task_state::link_unavailable, msg);
        co_return;
    }

    auto candidate_topics = find_candidate_topics();

    vlog(
      logger().trace,
      "Fetching topic configs for {}",
      std::views::keys(candidate_topics));

    kafka::describe_configs_response response;
    try {
        chunked_vector<::model::topic> topics_to_describe;
        topics_to_describe.reserve(candidate_topics.size());
        for (const auto& [topic, _] : candidate_topics) {
            topics_to_describe.emplace_back(topic);
        }
        response = co_await describe_topics(
          cluster,
          *controller_id,
          describe_configs_version,
          topics_to_describe,
          _config.topic_properties_to_mirror);
        vlog(logger().trace, "Describe topics response: {}", response);
    } catch (const std::exception& e) {
        auto msg = ssx::sformat("Failed to describe topics: {}", e.what());
        vlog(logger().warn, "{}", msg);
        std::ignore = change_state(
          model::task_state::link_unavailable, std::move(msg));
        co_return;
    }

    chunked_vector<model::add_mirror_topic_cmd> add_mirror_topic_cmds;
    add_mirror_topic_cmds.reserve(response.data.results.size());

    for (auto& resp : response.data.results) {
        vlog(
          logger().trace,
          "Processing results for topic: {}",
          resp.resource_name);
        if (resp.error_code != kafka::error_code::none) {
            vlog(
              logger().debug,
              "Failed to fetch configs for topic {}: {}{}",
              resp.resource_name,
              resp.error_code,
              resp.error_message.has_value() ? " - " + *resp.error_message
                                             : "");
            continue;
        }
        if (resp.resource_type != kafka::config_resource_type::topic) {
            vlog(
              logger().debug,
              "Unexpected resource type {} for topic {}",
              resp.resource_type,
              resp.resource_name);
            continue;
        }
        chunked_hash_map<ss::sstring, ss::sstring> configs;
        configs.reserve(resp.configs.size());
        for (auto& c : resp.configs) {
            if (c.value.has_value()) {
                configs.emplace(std::move(c.name), std::move(c.value).value());
            }
        }
        vlog(
          logger().trace,
          "Configs for topic {}: {}",
          resp.resource_name,
          configs);
        auto source_topic_name = ::model::topic{resp.resource_name};
        auto it = candidate_topics.find(source_topic_name);
        if (it == candidate_topics.end()) {
            vlog(
              logger().debug,
              "Topic {} not found in candidate topics, skipping",
              resp.resource_name);
            continue;
        }
        add_mirror_topic_cmds.emplace_back(model::add_mirror_topic_cmd{
          .topic = source_topic_name,
          .metadata = model::mirror_topic_metadata{
            .source_topic_name = source_topic_name,
            .destination_topic_id = ::model::topic_id{::uuid_t::create()},
            .partition_count = it->second.partition_count,
            .replication_factor = it->second.rf,
            .topic_configs = std::move(configs)}});
    }

    vlog(logger().trace, "Adding mirror topics");
    for (auto& c : add_mirror_topic_cmds) {
        auto topic_name = c.topic;
        vlog(logger().trace, "Adding mirror topic {}", topic_name);
        auto res = co_await get_link()->add_mirror_topic(std::move(c));
        if (res != ::cluster::cluster_link::errc::success) {
            vlog(logger().warn, "Failed to add mirror topic {}", res);
            continue;
        }
        vlog(logger().debug, "Successfully added mirror topic {}", topic_name);
    }

    if (get_state() != model::task_state::active) {
        std::ignore = change_state(
          model::task_state::active, "Auto topic sensor task completed");
    }
    vlog(logger().trace, "Auto topic sensor task completed");
}

chunked_hash_map<::model::topic, source_topic_syncer::topic_metadata>
source_topic_syncer::find_candidate_topics() {
    auto& cluster = get_link()->get_cluster_connection();
    auto& topic_cache = cluster.get_topics();
    auto topics = topic_cache.topics();

    /// Map of topics with partition count
    chunked_hash_map<::model::topic, topic_metadata> candidate_topics;
    candidate_topics.reserve(topics.size());

    for (const auto& topic : topics) {
        vlog(logger().trace, "Checking topic: {}", topic);

        const auto& topic_cache_map = topic_cache.cache();
        auto it = topic_cache_map.find(topic);
        if (it == topic_cache_map.end()) {
            vlog(logger().trace, "Skipping topic {} not in cache", topic);
            continue;
        }

        auto partition_count = it->second.partitions.size();
        if (partition_count < 1) {
            vlog(logger().trace, "Skipping topic {} with no partitions", topic);
            continue;
        }
        auto rf = it->second.replication_factor;
        if (rf < 1) {
            vlog(logger().trace, "Skipping topic {} with no replicas", topic);
            continue;
        }

        vlog(
          logger().trace,
          "Topic {} has {} partitions, RF={}",
          topic,
          partition_count,
          rf);

        if (get_link()
              ->get_topic_metadata_cache()
              ->find_topic_cfg({::model::kafka_namespace, topic})
              .has_value()) {
            vlog(logger().trace, "Topic {} already exists", topic);
            continue;
        }

        if (get_link()->config().state.mirror_topics.contains(topic)) {
            vlog(logger().trace, "Topic {} is already being mirrored", topic);
            continue;
        }

        vlog(
          logger().trace,
          "Checking topic {} against filters {}",
          topic,
          fmt::join(
            _config.topic_name_filters.begin(),
            _config.topic_name_filters.end(),
            ","));

        if (!::cluster_link::model::select_topic(
              topic, _config.topic_name_filters)) {
            vlog(
              logger().trace,
              "Topic {} does not match inclusion filters",
              topic);
            continue;
        }

        auto authorized_ops = it->second.authorized_operations;
        if (authorized_ops == kafka::topic_authorized_operations_not_set) {
            vlog(logger().trace, "Missing permissions for topic {}", topic);
            continue;
        }

        if (!has_required_permissions(authorized_ops, required_permissions)) {
            vlog(
              logger().trace,
              "Insufficient permissions for topic {}.  Requires {:08x}, has "
              "{:08x}",
              topic,
              required_permissions,
              authorized_ops);
            continue;
        }

        vlog(logger().debug, "Topic {} is candidate for mirroring", topic);
        candidate_topics.emplace(
          topic, topic_metadata{.partition_count = partition_count, .rf = rf});
    }

    return candidate_topics;
}

ss::future<kafka::describe_configs_response>
source_topic_syncer::describe_topics(
  kafka::client::cluster& cluster,
  ::model::node_id controller_id,
  kafka::api_version describe_configs_version,
  const chunked_vector<::model::topic>& topics,
  const absl::flat_hash_set<ss::sstring>& configs) {
    absl::flat_hash_set<ss::sstring> requested_configs_set = configs;
    requested_configs_set.insert(
      required_topic_properties.begin(), required_topic_properties.end());
    chunked_vector<ss::sstring> requested_configs(
      requested_configs_set.begin(), requested_configs_set.end());
    kafka::describe_configs_request request;
    request.data.include_documentation = false;
    request.data.include_synonyms = false;

    request.data.resources.reserve(topics.size());
    for (const auto& topic : topics) {
        kafka::describe_configs_resource resource;
        resource.resource_type = kafka::config_resource_type::topic;
        resource.resource_name = topic;
        resource.configuration_keys = requested_configs.copy();
        request.data.resources.emplace_back(std::move(resource));
    }

    co_return co_await cluster.dispatch_to(
      controller_id, std::move(request), describe_configs_version);
}

std::string_view
source_topic_syncer_factory::created_task_name() const noexcept {
    return source_topic_syncer::task_name;
}

std::unique_ptr<task> source_topic_syncer_factory::create_task(link* link) {
    return std::make_unique<source_topic_syncer>(link, link->get_config());
}
} // namespace cluster_link
