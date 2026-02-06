/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/topic_manifest_uploader.h"

#include "cloud_io/remote.h"
#include "cloud_storage/topic_manifest_state.h"
#include "cloud_storage/topic_path_provider.h"
#include "serde/rw/rw.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics {

topic_manifest_uploader::topic_manifest_uploader(
  ss::logger& log,
  const cloud_storage_clients::bucket_name& bucket,
  cloud_io::remote& remote)
  : log_(log)
  , bucket_(bucket)
  , remote_(remote) {}

ss::future<std::expected<void, topic_manifest_uploader::error>>
topic_manifest_uploader::upload_manifest(
  const cloud_storage::topic_path_provider& path_provider,
  const cluster::topic_configuration& cfg,
  model::initial_revision_id rev,
  retry_chain_node& retry_node) {
    auto path = path_provider.topic_manifest_path(cfg.tp_ns, rev);
    retry_chain_logger chain_log(log_, retry_node, path);

    cloud_storage::topic_manifest_state state{
      .cfg = cfg, .initial_revision = rev};
    auto buf = serde::to_iobuf(std::move(state));

    vlog(chain_log.debug, "Uploading topic manifest {}", cfg);
    auto res = co_await ss::coroutine::as_future(
      remote_.upload_object(cloud_io::upload_request{
        .transfer_details = {
          .bucket = bucket_,
          .key = cloud_storage_clients::object_key{path},
          .parent_rtc = retry_node,
        },
        .display_str = "topic_manifest",
        .payload = std::move(buf),
      }));

    if (res.failed()) {
        auto ex = res.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            co_return std::unexpected(
              error(errc::shutting_down, "Shutdown exception: {}", ex));
        }
        co_return std::unexpected(
          error(errc::io_error, "Upload exception: {}", ex));
    }

    auto upload_res = res.get();
    if (upload_res != cloud_io::upload_result::success) {
        co_return std::unexpected(
          error(errc::io_error, "Upload failed: {}", path));
    }

    vlog(chain_log.debug, "Topic manifest upload succeeded");
    co_return std::expected<void, error>{};
}

} // namespace cloud_topics
