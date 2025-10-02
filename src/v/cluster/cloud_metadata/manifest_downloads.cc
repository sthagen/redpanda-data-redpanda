/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/manifest_downloads.h"

#include "cloud_storage/remote.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/logger.h"
#include "utils/uuid.h"

#include <boost/uuid/uuid_io.hpp>

#include <optional>

namespace {

const std::regex cluster_metadata_manifest_prefix_expr{
  R"REGEX(cluster_metadata/[a-z0-9-]+/manifests/(\d+)/)REGEX"};

const std::regex cluster_metadata_manifest_expr{
  R"REGEX(cluster_metadata/([a-z0-9-]+)/manifests/(\d+)/cluster_manifest.json)REGEX"};

} // anonymous namespace

namespace cluster::cloud_metadata {

ss::future<cluster_manifest_result> download_highest_manifest_for_cluster(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node) {
    // Download the manifest
    auto cluster_uuid_prefix = cluster_manifests_prefix(cluster_uuid) + "/";
    vlog(
      clusterlog.trace, "Listing objects with prefix {}", cluster_uuid_prefix);
    auto list_res = co_await remote.list_objects(
      bucket,
      retry_node,
      cloud_storage_clients::object_key(cluster_uuid_prefix),
      '/');
    if (list_res.has_error()) {
        vlog(
          clusterlog.debug, "Error downloading manifest {}", list_res.error());
        co_return error_outcome::list_failed;
    }
    // Examine the metadata IDs for this cluster.
    // Results take the form:
    // cluster_metadata/<cluster_uuid>/manifests/<meta_id>/
    auto& manifest_prefixes = list_res.value().common_prefixes;
    cluster_metadata_manifest manifest;
    if (manifest_prefixes.empty()) {
        vlog(
          clusterlog.debug,
          "No manifests found for cluster {}",
          cluster_uuid());
        co_return error_outcome::no_matching_metadata;
    }
    for (const auto& prefix : manifest_prefixes) {
        vlog(
          clusterlog.trace, "Prefix found for {}: {}", cluster_uuid(), prefix);
    }
    // Find the manifest with the highest metadata ID.
    cluster_metadata_id highest_meta_id{};
    for (const auto& prefix : manifest_prefixes) {
        std::smatch matches;
        std::string p = prefix;
        // E.g. cluster_metadata/<cluster_uuid>/manifests/3/
        const auto matches_manifest_expr = std::regex_match(
          p.cbegin(), p.cend(), matches, cluster_metadata_manifest_prefix_expr);
        if (!matches_manifest_expr) {
            continue;
        }
        vassert(
          matches.size() >= 2,
          "Unexpected size of regex match",
          matches.size());
        const auto& meta_id_str = matches[1].str();
        cluster_metadata_id meta_id;
        try {
            meta_id = cluster_metadata_id(std::stol(meta_id_str.c_str()));
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Ignoring invalid metadata ID: {}",
              meta_id_str);
            continue;
        }
        highest_meta_id = std::max(highest_meta_id, meta_id);
    }
    if (highest_meta_id == cluster_metadata_id{}) {
        vlog(
          clusterlog.debug,
          "No manifests with valid metadata IDs found for cluster {}",
          cluster_uuid());
        co_return error_outcome::no_matching_metadata;
    }

    // Deserialize the manifest.
    auto manifest_res = co_await remote.download_manifest_json(
      bucket,
      cluster_manifest_key(cluster_uuid, highest_meta_id),
      manifest,
      retry_node);
    if (manifest_res != cloud_storage::download_result::success) {
        vlog(
          clusterlog.debug, "Manifest download failed with {}", manifest_res);
        co_return error_outcome::download_failed;
    }
    vlog(
      clusterlog.trace,
      "Downloaded manifest for {} from {}: {}",
      cluster_uuid(),
      bucket(),
      manifest);
    co_return manifest;
}

namespace {
bool is_offsets_snapshot_path(
  const ss::sstring& object, const cluster_metadata_manifest& m) {
    for (const auto& paths : m.offsets_snapshots_by_partition) {
        for (const auto& p : paths) {
            if (p == object) {
                return true;
            }
        }
    }
    return false;
}
} // anonymous namespace

ss::future<std::list<ss::sstring>> list_orphaned_by_manifest(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  const cluster_metadata_manifest& manifest,
  retry_chain_node& retry_node) {
    auto uuid_prefix = cluster_uuid_prefix(cluster_uuid) + "/";
    vlog(clusterlog.trace, "Listing objects with prefix {}", uuid_prefix);
    auto list_res = co_await remote.list_objects(
      bucket, retry_node, cloud_storage_clients::object_key(uuid_prefix));
    if (list_res.has_error()) {
        vlog(
          clusterlog.debug,
          "Error listing under {}: {}",
          uuid_prefix,
          list_res.error());
        co_return std::list<ss::sstring>{};
    }
    std::list<ss::sstring> ret;
    for (auto& item : list_res.value().contents) {
        if (
          item.key == ss::sstring{manifest.get_manifest_path()()}
          || item.key == manifest.controller_snapshot_path
          || is_offsets_snapshot_path(item.key, manifest)) {
            vlog(clusterlog.trace, "Ignoring expected object: {}", item.key);
            continue;
        }
        vlog(clusterlog.trace, "Found orphaned object: {}", item.key);
        ret.emplace_back(std::move(item.key));
    }
    co_return ret;
}

namespace {
ss::future<result<absl::btree_set<model::cluster_uuid>, error_outcome>>
find_candidate_uuids(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  const ss::sstring& cloud_storage_cluster_name,
  retry_chain_node& retry_node) {
    absl::btree_set<model::cluster_uuid> candidate_uuids;

    const auto cluster_uuids_prefix = cluster_name_ref_for_uuid_prefix_key(
      cloud_storage_cluster_name);
    vlog(
      clusterlog.trace, "Listing objects with prefix {}", cluster_uuids_prefix);
    auto uuids_list_res = co_await remote.list_objects(
      bucket, retry_node, cluster_uuids_prefix, std::nullopt);
    if (uuids_list_res.has_error()) {
        vlog(
          clusterlog.debug,
          "Error listing cluster UUIDs for cluster name {} in bucket {}: {}",
          cloud_storage_cluster_name,
          bucket(),
          uuids_list_res.error());
        co_return error_outcome::list_failed;
    }

    for (const auto& item : uuids_list_res.value().contents) {
        auto parsed = parse_cluster_name_ref_for_uuid_key(item.key);
        if (!parsed.has_value()) {
            vlog(
              clusterlog.warn,
              "Error parsing object key {} for cluster name {} in bucket {}: "
              "{}",
              item.key,
              cloud_storage_cluster_name,
              bucket(),
              parsed.error());
            co_return error_outcome::list_failed;
        }
        candidate_uuids.emplace(std::get<1>(parsed.value()));
    }

    co_return candidate_uuids;
}

using find_candidate_uuids_for_optional_name_result
  = result<std::optional<absl::btree_set<model::cluster_uuid>>, error_outcome>;

// If cloud_storage_cluster_name_opt is set, find candidate UUIDs for that name.
// If not set, check that the bucket does not contain any cluster name
// references, and return std::nullopt if so. If there are cluster name
// references in the bucket but no name is configured, return an error.
ss::future<find_candidate_uuids_for_optional_name_result>
find_candidate_uuids_for_optional_name(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  const std::optional<ss::sstring>& cloud_storage_cluster_name_opt,
  retry_chain_node& retry_node) {
    std::optional<absl::btree_set<model::cluster_uuid>> candidate_uuids;

    if (cloud_storage_cluster_name_opt.has_value()) {
        const auto& name = cloud_storage_cluster_name_opt.value();

        auto candidate_uuids_res = co_await find_candidate_uuids(
          remote, bucket, name, retry_node);
        if (!candidate_uuids_res.has_value()) {
            co_return candidate_uuids_res.error();
        }

        if (candidate_uuids_res.value().empty()) {
            vlog(
              clusterlog.debug,
              "No cluster UUIDs found for cluster name {} in bucket {}",
              name,
              bucket());
            co_return error_outcome::no_matching_metadata;
        }

        candidate_uuids.emplace(std::move(candidate_uuids_res.value()));

        vlog(
          clusterlog.trace,
          "Found candidate UUIDs: {}",
          fmt::join(candidate_uuids.value(), ", "));

        co_return std::move(candidate_uuids);
    } else {
        // If cluster name is not configured we need to error out if the bucket
        // contains any cluster name references to avoid customer shooting
        // themselves in the foot.
        auto uses_cluster_id_res = co_await check_bucket_contains_cluster_names(
          remote, bucket, retry_node);
        if (!uses_cluster_id_res.has_value()) {
            vlog(
              clusterlog.debug,
              "Error checking for cluster names in bucket {}: {}",
              bucket(),
              uses_cluster_id_res.error());
            co_return error_outcome::list_failed;
        }
        if (uses_cluster_id_res.value()) {
            vlog(
              clusterlog.warn,
              "Cluster name references exist in bucket {}, but no cluster name "
              "is configured. Cannot proceed. Please check "
              "`cloud_storage_cluster_name` config property.",
              bucket());
            co_return error_outcome::misconfiguration;
        }

        co_return std::nullopt;
    }
}

} // namespace

ss::future<cluster_manifest_result> download_highest_manifest_in_bucket(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node,
  const cluster_name_filter& cluster_name_filter,
  std::optional<model::cluster_uuid> ignore_uuid) {
    auto candidate_uuids_res = co_await ss::visit(
      cluster_name_filter,
      [](const cluster_name_ignore_t&)
        -> ss::future<find_candidate_uuids_for_optional_name_result> {
          return ss::make_ready_future<
            find_candidate_uuids_for_optional_name_result>(std::nullopt);
      },
      [&remote, &bucket, &retry_node](
        const std::optional<ss::sstring>& cluster_name_opt)
        -> ss::future<find_candidate_uuids_for_optional_name_result> {
          return find_candidate_uuids_for_optional_name(
            remote, bucket, cluster_name_opt, retry_node);
      });

    if (candidate_uuids_res.has_error()) {
        co_return candidate_uuids_res.error();
    }
    std::optional<absl::btree_set<model::cluster_uuid>> candidate_uuids
      = std::move(candidate_uuids_res.value());

    // Look for unique cluster UUIDs for which we have metadata.
    constexpr auto cluster_prefix = "cluster_metadata/";
    vlog(clusterlog.trace, "Listing objects with prefix {}", cluster_prefix);
    auto list_res = co_await remote.list_objects(
      bucket,
      retry_node,
      cloud_storage_clients::object_key(cluster_prefix),
      std::nullopt);
    if (list_res.has_error()) {
        vlog(clusterlog.debug, "Error downloading manifest", list_res.error());
        co_return error_outcome::list_failed;
    }
    // Examine all cluster metadata in this bucket.
    auto& cluster_metadata_items = list_res.value().contents;
    if (cluster_metadata_items.empty()) {
        vlog(clusterlog.debug, "No manifests found in bucket {}", bucket());
        co_return error_outcome::no_matching_metadata;
    }

    // Look through those that look like cluster metadata manifests and find
    // the one with the highest metadata ID. This will be the returned to the
    // caller.
    model::cluster_uuid uuid_with_highest_meta_id{};
    cluster_metadata_id highest_meta_id{};
    for (const auto& item : cluster_metadata_items) {
        std::smatch matches;
        std::string k = item.key;
        const auto matches_manifest_expr = std::regex_match(
          k.cbegin(), k.cend(), matches, cluster_metadata_manifest_expr);
        if (!matches_manifest_expr) {
            continue;
        }
        const auto& cluster_uuid_str = matches[1].str();
        const auto& meta_id_str = matches[2].str();
        cluster_metadata_id meta_id{};
        model::cluster_uuid cluster_uuid{};
        try {
            meta_id = cluster_metadata_id(std::stoi(meta_id_str.c_str()));
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Ignoring invalid metadata ID: {}",
              meta_id_str);
            continue;
        }
        try {
            auto u = boost::lexical_cast<uuid_t::underlying_t>(
              cluster_uuid_str);
            std::vector<uint8_t> uuid_vec{u.begin(), u.end()};
            cluster_uuid = model::cluster_uuid(std::move(uuid_vec));
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Ignoring invalid cluster UUID: {}",
              cluster_uuid_str);
            continue;
        }
        if (
          ignore_uuid == cluster_uuid
          || (candidate_uuids.has_value() && !candidate_uuids.value().contains(cluster_uuid))) {
            continue;
        }
        if (meta_id > highest_meta_id) {
            highest_meta_id = meta_id;
            uuid_with_highest_meta_id = cluster_uuid;
        }
    }
    if (highest_meta_id == cluster_metadata_id{}) {
        vlog(clusterlog.debug, "No valid manifests in bucket {}", bucket());
        co_return error_outcome::no_matching_metadata;
    }
    cluster_metadata_manifest manifest;
    auto manifest_res = co_await remote.download_manifest_json(
      bucket,
      cluster_manifest_key(uuid_with_highest_meta_id, highest_meta_id),
      manifest,
      retry_node);
    if (manifest_res != cloud_storage::download_result::success) {
        vlog(
          clusterlog.debug, "Manifest download failed with {}", manifest_res);
        co_return error_outcome::download_failed;
    }
    co_return manifest;
}

ss::future<std::expected<bool, std::string>>
check_bucket_contains_cluster_names(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node) {
    // Reasonably high number of attempts but bounded so we don't loop
    // forever/overspend on cloud storage costs.
    constexpr auto max_attempts = 100;

    std::optional<ss::sstring> continuation_token;
    for (auto attempt = 0; attempt < max_attempts; ++attempt) {
        auto list_res = co_await remote.list_objects(
          bucket,
          retry_node,
          cluster_name_prefix_key(),
          std::nullopt,
          std::nullopt,
          1,
          continuation_token);
        if (list_res.has_error()) {
            co_return std::unexpected(
              fmt::format("Error listing cluster names: {}", list_res.error()));
        }
        if (list_res.value().next_continuation_token.empty()) {
            // If there are no more pages then this result is final.
            co_return !list_res.value().contents.empty();
        }
        if (!list_res.value().contents.empty()) {
            // If there are any results at all then we know the bucket uses
            // cluster ids.
            co_return true;
        }
        // Otherwise, we need to keep looking. An empty page with a
        // continuation token means there are more results to fetch.
        continuation_token = list_res.value().next_continuation_token;
    }

    co_return std::unexpected(
      "Exceeded maximum attempts at listing objects for determining use of WCR "
      "cluster names");
}

ss::future<std::expected<bool, std::string>> check_cluster_name_owns_uuid(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  const ss::sstring& cluster_name,
  const model::cluster_uuid& cluster_uuid,
  retry_chain_node& retry_node) {
    auto td = cloud_io::transfer_details{
      .bucket = bucket,
      .key = cluster_name_ref_for_uuid_key(cluster_name, cluster_uuid),
      .parent_rtc = retry_node,
    };
    iobuf payload;
    auto download_result = co_await remote.download_object({
      .transfer_details = std::move(td),
      .type = cloud_storage::download_type::object,
      .payload = payload,
    });
    if (download_result == cloud_storage::download_result::success) {
        co_return true;
    } else if (download_result == cloud_storage::download_result::notfound) {
        co_return false;
    }
    co_return std::unexpected(
      fmt::format(
        "Error checking cluster name ownership: {}", download_result));
}

} // namespace cluster::cloud_metadata
