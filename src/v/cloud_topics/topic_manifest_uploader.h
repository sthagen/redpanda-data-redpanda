/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_storage_clients/types.h"
#include "cluster/topic_configuration.h"
#include "model/fundamental.h"
#include "utils/detailed_error.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <expected>

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_storage {
class topic_path_provider;
} // namespace cloud_storage

namespace cloud_topics {

// Encapsulates individual uploads of topic manifests within a given bucket and
// remote. The uploads here match those used by tiered storage, and are
// expected to be compatible with the cloud_storage::topic_manifest_downloader.
class topic_manifest_uploader {
public:
    enum class errc {
        io_error,
        shutting_down,
    };
    using error = detailed_error<errc>;

    topic_manifest_uploader(
      ss::logger&,
      const cloud_storage_clients::bucket_name& bucket,
      cloud_io::remote&);

    ss::future<std::expected<void, error>> upload_manifest(
      const cloud_storage::topic_path_provider& path_provider,
      const cluster::topic_configuration& cfg,
      model::initial_revision_id rev,
      retry_chain_node& retry_node);

private:
    ss::logger& log_;
    const cloud_storage_clients::bucket_name bucket_;
    cloud_io::remote& remote_;
};

} // namespace cloud_topics

template<>
struct fmt::formatter<cloud_topics::topic_manifest_uploader::errc>
  : fmt::formatter<std::string_view> {
    auto format(
      cloud_topics::topic_manifest_uploader::errc e,
      fmt::format_context& ctx) const {
        using errc = cloud_topics::topic_manifest_uploader::errc;
        std::string_view name;
        switch (e) {
        case errc::io_error:
            name = "io_error";
            break;
        case errc::shutting_down:
            name = "shutting_down";
            break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};
