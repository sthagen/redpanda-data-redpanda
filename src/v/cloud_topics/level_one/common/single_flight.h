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

#include "base/vlog.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

#include <expected>
#include <filesystem>
#include <optional>
#include <type_traits>

namespace cloud_topics::l1 {

/// Per-shard single-flight coordinator: dedups concurrent operations
/// that share a cloud cache key so only one "leader" caller runs the
/// work and the rest merge onto a shared future for the leader's
/// outcome.
///
/// The optimization is opportunistic, so the map size is bounded.
/// Beyond max_entries, callers run their work uncoordinated. Whatever
/// operation the work performs should have its own concurrency control
/// mechanism downstream.
class single_flight {
public:
    /// Internal promise/future payload: the work succeeded, or it
    /// failed with this errc.
    using outcome = std::expected<void, io::errc>;

    /// What a caller of run() observes.
    ///
    /// Success branch: bool -- true iff this caller joined an
    /// existing leader (did not run work itself). False in the leader
    /// and uncoordinated (at-capacity) cases.
    ///
    /// Error branch: the work's errc. Could be any of:
    ///  - a merger whose abort_source fired while waiting
    ///  - a merger whose leader failed
    ///  - a worker (leader or uncoordinated) that failed
    /// In any of these cases, callers should treat this result as failed work
    /// and take appropriate measures to retry, propagate the error, etc.
    using run_result = std::expected<bool, io::errc>;

    /// Default bound on concurrent distinct keys tracked per shard.
    static constexpr size_t default_max_entries = 4096;

    explicit single_flight(size_t max_entries = default_max_entries) noexcept
      : _max_entries(max_entries) {}

    /// Run work under single-flight coordination for cache_key.
    ///
    /// - If no caller on this shard is currently running work for
    ///   cache_key, become the leader: insert an entry, run work,
    ///   publish its outcome to any concurrent mergers, then erase
    ///   the entry. Returns success(false).
    /// - If another caller is already running work for cache_key,
    ///   await the leader's outcome and return success(true).
    /// - If the map is at max_entries, run work uncoordinated (no
    ///   entry inserted, no mergers possible). Returns success(false).
    /// - On any work failure (leader's own, inherited by mergers, or
    ///   merger-side abort), returns unexpected(errc).
    ///
    /// If `as` is already aborted on entry, run returns
    /// unexpected(cloud_op_timeout) immediately. It neither
    /// coordinates nor runs work, so a cancelled caller never becomes a
    /// leader that healthy mergers would then inherit a failure from.
    ///
    /// run never throws and reports every failure through run_result's
    /// error branch. If work's returned future fails, we propagate
    /// errc::file_io_error to this caller and any waiting mergers, and
    /// log the exception via logger, if present.
    ///
    /// work must be invocable as ss::future<outcome>(). It runs
    /// at most once per call to run.
    ///
    /// cache_key is the cloud cache path the work reads/writes, not
    /// the remote object id. It encodes the byte range, so distinct
    /// ranges of one object are distinct keys and don't coalesce into
    /// one flight.
    template<typename WorkFn>
    requires std::is_invocable_r_v<ss::future<outcome>, WorkFn>
    ss::future<run_result> run(
      std::filesystem::path cache_key,
      ss::abort_source& as,
      WorkFn work,
      ss::logger* logger = nullptr) noexcept;

    size_t in_flight() const noexcept { return _entries.size(); }
    size_t capacity() const noexcept { return _max_entries; }

private:
    /// Outcome of the synchronous lookup-or-insert step inside run.
    enum class join_kind { leader, merger, at_capacity };
    struct join_result {
        join_kind kind;
        /// Engaged iff kind == merger. ss::future is not default
        /// constructible, hence the optional wrapper.
        std::optional<ss::future<outcome>> merge_future;
    };

    /// Synchronous lookup-or-insert step.
    join_result
    join_or_lead(const std::filesystem::path& cache_key, ss::abort_source& as);

    /// Publish an outcome to mergers waiting on cache_key and erase the
    /// entry. Called only the leader fiber, exactly once per flight.
    void
    release_leader(const std::filesystem::path& cache_key, outcome o) noexcept;

    chunked_hash_map<std::filesystem::path, ss::shared_promise<outcome>>
      _entries;
    size_t _max_entries;
};

template<typename WorkFn>
requires std::is_invocable_r_v<ss::future<single_flight::outcome>, WorkFn>
ss::future<single_flight::run_result> single_flight::run(
  std::filesystem::path cache_key,
  ss::abort_source& as,
  WorkFn work,
  ss::logger* logger) noexcept {
    if (as.abort_requested()) {
        co_return std::unexpected(io::errc::cloud_op_timeout);
    }

    auto j = join_or_lead(cache_key, as);

    if (j.kind == join_kind::merger) {
        auto fut = co_await ss::coroutine::as_future(
          std::move(*j.merge_future));
        if (fut.failed()) {
            // The merger's own abort_source fired while waiting on
            // the leader. The leader will still publish its outcome
            // later; we just surface cloud_op_timeout to this caller.
            fut.ignore_ready_future();
            co_return std::unexpected(io::errc::cloud_op_timeout);
        }
        // Inherit the leader's outcome: propagate its errc, or report
        // a successful merge.
        auto leader_outcome = fut.get();
        if (!leader_outcome.has_value()) {
            co_return std::unexpected(leader_outcome.error());
        }
        co_return true; // success (merged onto an existing leader)
    }

    // Leader or at-capacity uncoordinated: this caller runs work.
    const bool is_leader = j.kind == join_kind::leader;
    outcome work_outcome = std::unexpected(io::errc::file_io_error);

    auto work_fut = co_await ss::coroutine::as_future(work());
    if (work_fut.failed()) {
        // work is meant to encode failure in its outcome, not raise. If
        // it does raise, leave work_outcome at file_io_error so callers
        // see one failure channel and the leader still releases mergers.
        auto eptr = work_fut.get_exception();
        if (logger != nullptr) {
            vlog(
              logger->warn,
              "single_flight work for {} raised: {}",
              cache_key,
              eptr);
        }
    } else {
        work_outcome = work_fut.get();
    }

    if (is_leader) {
        release_leader(cache_key, work_outcome);
    }
    if (!work_outcome.has_value()) {
        co_return std::unexpected(work_outcome.error());
    }
    co_return false; // work succeeded, did not merge
}

} // namespace cloud_topics::l1
