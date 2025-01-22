/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/iceberg_snapshot_remover.h"

#include "base/seastarx.h"
#include "base/vlog.h"
#include "datalake/logger.h"
#include "iceberg/catalog.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/transaction.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace datalake::coordinator {
namespace {
snapshot_remover::errc log_and_convert_catalog_errc(
  prefix_logger& log, iceberg::catalog::errc e, std::string_view msg) {
    switch (e) {
        using enum iceberg::catalog::errc;
    case shutting_down:
        vlog(log.debug, "{}: {}", msg, e);
        return snapshot_remover::errc::shutting_down;
    case timedout:
    case io_error:
    case unexpected_state:
    case already_exists:
    case not_found:
        vlog(log.warn, "{}: {}", msg, e);
        return snapshot_remover::errc::failed;
    }
}
snapshot_remover::errc log_and_convert_action_errc(
  prefix_logger& log, iceberg::action::errc e, std::string_view msg) {
    switch (e) {
        using enum iceberg::action::errc;
    case shutting_down:
        vlog(log.debug, "{}: {}", msg, e);
        return snapshot_remover::errc::shutting_down;
    case unexpected_state:
    case io_failed:
        vlog(log.warn, "{}: {}", msg, e);
        return snapshot_remover::errc::failed;
    }
}
} // anonymous namespace

ss::future<checked<std::nullopt_t, snapshot_remover::errc>>
iceberg_snapshot_remover::remove_expired_snapshots(
  model::topic topic, model::timestamp ts) const {
    auto table_id = table_id_for_topic(topic);
    prefix_logger log(
      datalake_log, fmt::format("[topic: {}, table_id: {}]", topic, table_id));
    auto table_res = co_await catalog_.load_table(table_id);
    if (table_res.has_error()) {
        co_return log_and_convert_catalog_errc(
          log,
          table_res.error(),
          "Error loading table before expiring snapshots");
    }
    chunked_hash_map<iceberg::snapshot_id, iceberg::uri> before_snapshots;
    auto& before_table = table_res.value();
    if (before_table.snapshots.has_value()) {
        for (const auto& s : *before_table.snapshots) {
            before_snapshots.emplace(s.id, s.manifest_list_path);
        }
    }

    iceberg::transaction txn(std::move(before_table));
    auto removal_res = co_await txn.remove_expired_snapshots(ts);
    if (removal_res.has_error()) {
        co_return log_and_convert_action_errc(
          log, removal_res.error(), "Error computing expired snapshots");
    }
    auto commit_res = co_await catalog_.commit_txn(table_id, std::move(txn));
    if (commit_res.has_error()) {
        co_return log_and_convert_catalog_errc(
          log, commit_res.error(), "Error committing snapshot removal");
    }

    // Reload the table to get the snapshots that were actually removed.
    auto after_table_res = co_await catalog_.load_table(table_id);
    if (after_table_res.has_error()) {
        co_return log_and_convert_catalog_errc(
          log,
          after_table_res.error(),
          "Error loading table after expiring snapshots");
    }
    auto& after_table = after_table_res.value();
    auto snapshots_pending_removal = std::move(before_snapshots);
    if (after_table.snapshots.has_value()) {
        for (const auto& s : *after_table.snapshots) {
            snapshots_pending_removal.erase(s.id);
        }
    }

    // Collect the snapshots that have been removed in this operation.
    // TODO: we should consider doing cleanup in the background. At this point,
    // the removal has already succeeded.
    chunked_vector<std::filesystem::path> paths_to_remove;
    for (const auto& [_, uri] : snapshots_pending_removal) {
        auto path_res = io_.from_uri(uri);
        if (path_res.has_error()) {
            // NOTE: errors are already logged in from_uri().
            continue;
        }
        paths_to_remove.emplace_back(path_res.value());
    }
    if (paths_to_remove.empty()) {
        vlog(log.debug, "No valid paths removed, skipping cleanup");
        co_return std::nullopt;
    }

    ss::abort_source as;
    using namespace std::chrono_literals;
    retry_chain_node retry(as, 30s, 1s);
    auto delete_res = co_await io_.delete_files(
      std::move(paths_to_remove), retry);
    if (delete_res.has_error()) {
        switch (delete_res.error()) {
            using enum iceberg::metadata_io::errc;
        case shutting_down:
            co_return errc::shutting_down;
        case failed:
        case invalid_uri:
        case timedout:
            co_return errc::failed;
        }
    }
    co_return std::nullopt;
}

iceberg::table_identifier
iceberg_snapshot_remover::table_id_for_topic(const model::topic& t) const {
    return iceberg::table_identifier{
      // TODO: namespace as a topic property? Keep it in the table metadata?
      .ns = {"redpanda"},
      .table = t,
    };
}

} // namespace datalake::coordinator
