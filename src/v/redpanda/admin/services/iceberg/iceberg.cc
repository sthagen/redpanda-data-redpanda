/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/services/iceberg/iceberg.h"

#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/types.h"
#include "iceberg/catalog_errors.h"

#include <optional>

namespace {

proto::admin::iceberg_lifecycle_state
to_proto(datalake::coordinator::topic_state::lifecycle_state_t state) {
    using lifecycle = datalake::coordinator::topic_state::lifecycle_state_t;
    switch (state) {
    case lifecycle::live:
        return proto::admin::iceberg_lifecycle_state::live;
    case lifecycle::closed:
        return proto::admin::iceberg_lifecycle_state::closed;
    case lifecycle::purged:
        return proto::admin::iceberg_lifecycle_state::purged;
    }
    return proto::admin::iceberg_lifecycle_state::unspecified;
}

// Maps one coordinator partition_state onto the public per-partition status,
// computing the highest translated offset and commit lag server-side.
proto::admin::partition_iceberg_status
to_proto(int32_t pid, const datalake::coordinator::partition_state& ps) {
    proto::admin::partition_iceberg_status out;
    out.set_partition(pid);

    // Highest translated offset: the last pending entry's last offset if any
    // are pending, else the last committed offset, else nothing translated.
    std::optional<int64_t> translated;
    if (!ps.pending_entries.empty()) {
        translated = ps.pending_entries.back().data.last_offset();
    } else if (ps.last_committed.has_value()) {
        translated = ps.last_committed.value()();
    }

    std::optional<int64_t> committed;
    if (ps.last_committed.has_value()) {
        committed = ps.last_committed.value()();
    }

    if (translated.has_value()) {
        out.set_last_translated_offset(translated.value());
    }
    if (committed.has_value()) {
        out.set_last_committed_offset(committed.value());
    }

    // Commit lag: offsets in Parquet but not yet committed to the catalog.
    int64_t commit_lag = 0;
    if (translated.has_value()) {
        if (committed.has_value()) {
            commit_lag = translated.value() - committed.value();
        } else if (!ps.pending_entries.empty()) {
            auto first_start = ps.pending_entries.front().data.start_offset();
            commit_lag = translated.value() - first_start + 1;
        } else {
            commit_lag = translated.value() + 1;
        }
    }
    out.set_commit_lag(commit_lag);
    return out;
}

} // namespace

namespace admin {

iceberg_service_impl::iceberg_service_impl(
  ss::sharded<datalake::coordinator::frontend>* coordinator_fe)
  : _coordinator_fe(coordinator_fe) {}

ss::future<proto::admin::get_iceberg_status_response>
iceberg_service_impl::get_iceberg_status(
  serde::pb::rpc::context, proto::admin::get_iceberg_status_request req) {
    if (!_coordinator_fe->local_is_initialized()) {
        throw serde::pb::rpc::unavailable_exception(
          "Datalake coordinator frontend not initialized");
    }

    // Group topics by coordinator partition (empty filter = all topics).
    chunked_hash_map<model::partition_id, chunked_vector<model::topic>>
      coordinator_partition_to_topics;
    if (req.get_topics_filter().empty()) {
        auto count = _coordinator_fe->local().coordinator_partition_count();
        if (!count.has_value()) {
            throw serde::pb::rpc::unavailable_exception(
              "Datalake coordinator couldn't get coordinator partition count");
        }
        for (auto p = 0; p < count.value(); ++p) {
            coordinator_partition_to_topics.emplace(
              model::partition_id{p}, chunked_vector<model::topic>());
        }
    } else {
        for (const auto& name : req.get_topics_filter()) {
            model::topic topic{name};
            auto p = _coordinator_fe->local().coordinator_partition(topic);
            if (!p.has_value()) {
                throw serde::pb::rpc::unavailable_exception(
                  fmt::format(
                    "Datalake coordinator couldn't get coordinator partition "
                    "for {}",
                    topic));
            }
            coordinator_partition_to_topics[p.value()].emplace_back(
              std::move(topic));
        }
    }

    proto::admin::get_iceberg_status_response resp;

    // Per-topic translation state, mapped from the coordinator's native state.
    chunked_vector<proto::admin::topic_iceberg_status> topics;
    for (auto& [partition_id, filter] : coordinator_partition_to_topics) {
        datalake::coordinator::get_topic_state_request fe_req{
          partition_id, std::move(filter)};
        auto fe_res = co_await _coordinator_fe->local().get_topic_state(
          std::move(fe_req));
        if (fe_res.errc != datalake::coordinator::errc::ok) {
            throw serde::pb::rpc::internal_exception(
              fmt::format(
                "Datalake coordinator error for partition {}: {}",
                partition_id,
                fe_res.errc));
        }
        for (auto& [topic, state] : fe_res.topic_states) {
            proto::admin::topic_iceberg_status t;
            t.set_topic(ss::sstring(topic()));
            t.set_lifecycle_state(to_proto(state.lifecycle_state));

            chunked_vector<proto::admin::partition_iceberg_status> partitions;
            for (const auto& [pid, ps] : state.pid_to_pending_files) {
                partitions.emplace_back(to_proto(pid(), ps));
            }
            t.set_partitions(std::move(partitions));
            topics.emplace_back(std::move(t));
        }
    }
    resp.set_topics(std::move(topics));

    // Catalog reachability: probe the running cluster config. Empty error
    // fields mean reachable.
    proto::admin::catalog_health catalog;
    auto cat = co_await _coordinator_fe->local().describe_catalog();
    if (cat.has_error()) {
        const auto& err = cat.error();
        catalog.set_reachable(false);
        catalog.set_error_code(ss::sstring(iceberg::to_string_view(err.errc)));
        catalog.set_error_message(ss::sstring(err.message));
    } else {
        catalog.set_reachable(true);
    }
    resp.set_catalog(std::move(catalog));

    co_return resp;
}

} // namespace admin
