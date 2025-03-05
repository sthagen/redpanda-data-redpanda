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

#include "cluster/fwd.h"
#include "cluster/metrics_reporter.h"
#include "crash_tracker/recorder.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <vector>

namespace cluster {

class crash_reporter {
public:
    struct crash_report_payload {
        struct report {
            uint64_t timestamp{0};
            model::node_id node_id;
            ss::sstring stacktrace;
            ss::sstring reason;
            ss::sstring description;
            ss::sstring app_version;
        };

        ss::sstring cluster_uuid;
        std::vector<report> items;
    };

    static constexpr ss::shard_id shard = 0;

    crash_reporter(
      ss::sharded<controller_stm>&,
      ss::sharded<ss::abort_source>&,
      ss::sharded<metrics_reporter>&);

    ss::future<> start();
    ss::future<> stop();

private:
    using report_batch = std::vector<crash_tracker::recorder::recorded_crash>;
    ss::future<> report_crashes();
    ss::future<bool> try_report_crashes(const report_batch&);
    ss::future<crash_report_payload>
    build_crash_report_payload(const report_batch&);
    iobuf serialize_payload(const crash_report_payload&);

    ss::sharded<controller_stm>& _controller_stm;
    ss::sharded<metrics_reporter>& _metrics_reporter;
    ss::sharded<ss::abort_source>& _as;
    details::address _address;
    prefix_logger _client_logger;
    ss::gate _gate;
};
} // namespace cluster
namespace json {
void rjson_serialize(
  json::Writer<json::StringBuffer>&,
  const cluster::crash_reporter::crash_report_payload&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&,
  const cluster::crash_reporter::crash_report_payload::report&);
} // namespace json
