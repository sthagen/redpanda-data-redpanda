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

#pragma once

#include "base/seastarx.h"
#include "kafka/server/consumer_group_lag_metrics_rpc_service.h"
#include "kafka/server/fwd.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace kafka {

/**
 * \brief Dispatch requests to partition leaders.
 */
class consumer_group_lag_metrics_service final
  : public consumer_group_lag_metrics_rpc_service {
public:
    consumer_group_lag_metrics_service(
      ss::scheduling_group sched_group,
      ss::smp_service_group smp_group,
      ss::sharded<consumer_group_lag_metrics_frontend>& frontend)
      : consumer_group_lag_metrics_rpc_service(sched_group, smp_group)
      , _frontend{frontend} {}
    ~consumer_group_lag_metrics_service() override = default;

    ss::future<partition_offsets_reply> partition_offsets(
      partition_offsets_request, ::rpc::streaming_context&) override;

private:
    ss::sharded<consumer_group_lag_metrics_frontend>& _frontend;
};

} // namespace kafka
