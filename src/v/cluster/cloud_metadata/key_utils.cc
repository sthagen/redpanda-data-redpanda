/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/key_utils.h"

#include "cluster/cloud_metadata/types.h"
#include "model/fundamental.h"

namespace cluster::cloud_metadata {

constexpr std::string_view prefix = "cluster_metadata";

ss::sstring cluster_uuid_prefix(const model::cluster_uuid& cluster_uuid) {
    return fmt::format("{}/{}", prefix, ss::sstring(cluster_uuid()));
}

ss::sstring cluster_manifests_prefix(const model::cluster_uuid& cluster_uuid) {
    return fmt::format("{}/manifests", cluster_uuid_prefix(cluster_uuid));
}

cloud_storage::remote_manifest_path cluster_manifest_key(
  const model::cluster_uuid& cluster_uuid, const cluster_metadata_id& meta_id) {
    return cloud_storage::remote_manifest_path(
      fmt::format(
        "{}/{}/cluster_manifest.json",
        cluster_manifests_prefix(cluster_uuid),
        meta_id()));
}

cloud_storage::remote_segment_path controller_snapshot_key(
  const model::cluster_uuid& cluster_uuid, const model::offset& offset) {
    return cloud_storage::remote_segment_path(
      fmt::format(
        "{}/{}/controller.snapshot",
        cluster_uuid_prefix(cluster_uuid),
        offset()));
}

ss::sstring cluster_metadata_prefix(
  const model::cluster_uuid& cluster_uuid, const cluster_metadata_id& meta_id) {
    return fmt::format("{}/{}", cluster_uuid_prefix(cluster_uuid), meta_id());
}

cloud_storage_clients::object_key offsets_snapshot_key(
  const model::cluster_uuid& cluster_uuid,
  const cluster_metadata_id& meta_id,
  const model::partition_id& pid,
  size_t snapshot_idx) {
    return cloud_storage_clients::object_key{fmt::format(
      "{}/{}/offsets/{}/{}.snapshot",
      cluster_uuid_prefix(cluster_uuid),
      meta_id(),
      pid(),
      snapshot_idx)};
}

cloud_storage_clients::object_key cluster_name_ref_for_uuid_key(
  const ss::sstring& name, const model::cluster_uuid& cluster_uuid) {
    return cloud_storage_clients::object_key{fmt::format(
      "cluster_name/{}/uuid/{}", name, ss::sstring(cluster_uuid()))};
}

namespace {

const std::regex cluster_uuid_by_name_expr{
  R"REGEX(^cluster_name/([a-zA-Z0-9_\-]+)/uuid/([a-z0-9\-]+)$)REGEX"};

} // namespace

std::expected<std::tuple<ss::sstring, model::cluster_uuid>, std::string>
parse_cluster_name_ref_for_uuid_key(const std::string& key) {
    std::smatch matches;
    const auto match_ok = std::regex_match(
      key.begin(), key.end(), matches, cluster_uuid_by_name_expr);
    if (!match_ok || matches.size() < 3) {
        return std::unexpected(
          fmt::format("Key does not match expected format: {}", key));
    }
    const auto& cluster_name = matches[1].str();
    const auto& cluster_uuid_str = matches[2].str();
    model::cluster_uuid cluster_uuid{};
    try {
        cluster_uuid = model::cluster_uuid(
          uuid_t::from_string(cluster_uuid_str));
    } catch (...) {
        return std::unexpected(
          fmt::format("Invalid cluster UUID: {}", cluster_uuid_str));
    }
    return std::make_tuple(ss::sstring(cluster_name), cluster_uuid);
}

cloud_storage_clients::object_key
cluster_name_ref_for_uuid_prefix_key(const ss::sstring& name) {
    return cloud_storage_clients::object_key{
      fmt::format("cluster_name/{}/uuid/", name)};
}

} // namespace cluster::cloud_metadata
