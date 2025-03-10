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

#include "consumer_group_lag_metrics_service.h"

#include "consumer_group_lag_metrics_frontend.h"
#include "kafka/server/logger.h"

namespace {}

namespace kafka {

ss::future<partition_offsets_reply>
consumer_group_lag_metrics_service::partition_offsets(
  partition_offsets_request req, ::rpc::streaming_context&) {
    vlog(
      klog.debug,
      "consumer_group_lag_metrics_service::partition_offsets: {}",
      req.data.size());
    if (!_frontend.local_is_initialized()) {
        vlog(
          klog.debug,
          "consumer_group_lag_metrics_service::partition_offsets: "
          "frontend not initialized");
        co_return partition_offsets_reply{};
    }
    co_return co_await _frontend.local().get_local_partition_offsets(
      std::move(req));
}

} // namespace kafka
