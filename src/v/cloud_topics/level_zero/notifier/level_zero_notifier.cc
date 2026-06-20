/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/notifier/level_zero_notifier.h"

#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/logger.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace cloud_topics {

namespace {
constexpr auto replicate_timeout = std::chrono::seconds{30};
} // namespace

level_zero_notifier::level_zero_notifier(
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cluster::partition_manager>* partition_manager,
  std::chrono::milliseconds retry_backoff)
  : _shard_table(shard_table)
  , _partition_manager(partition_manager)
  , _retry_backoff(retry_backoff) {}

ss::future<> level_zero_notifier::stop() {
    _as.request_abort();
    _inflight.broken();
    co_await _gate.close();
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::set_min_allowed_local_threshold(
  model::ntp ntp, kafka::offset new_floor) {
    vlog(
      cd_log.debug,
      "{} set_min_allowed_local_threshold called, the new floor {}",
      ntp,
      new_floor);

    auto shard = _shard_table->local().shard_for(ntp);
    if (!shard.has_value()) {
        // Partition is not hosted on this node: nothing to replicate.
        co_return std::unexpected(ctp_stm_api_errc::not_leader);
    }
    auto fut = co_await ss::coroutine::as_future(
      container().invoke_on(
        *shard, [ntp, new_floor](level_zero_notifier& self) mutable {
            return self.replicate_on_home_shard(std::move(ntp), new_floor);
        }));

    if (fut.failed()) {
        auto err = fut.get_exception();
        auto errc = ctp_stm_api_errc::failure;
        if (ssx::is_shutdown_exception(err)) {
            vlog(
              cd_log.debug,
              "{} set_min_allowed_local_threshold failed due to shutdown for "
              "new floor "
              "{}",
              ntp,
              new_floor);
            errc = ctp_stm_api_errc::shutdown;
        } else {
            vlog(
              cd_log.warn,
              "{} set_min_allowed_local_threshold failed for new floor {}, "
              "error {}",
              ntp,
              new_floor,
              err);
        }
        co_return std::unexpected(errc);
    }
    co_return fut.get();
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::replicate_on_home_shard(
  model::ntp ntp, kafka::offset new_floor) {
    auto holder = _gate.hold();
    auto units = co_await ss::get_units(_inflight, 1);

    auto partition = _partition_manager->local().get(ntp);
    if (!partition) {
        vlog(cd_log.info, "{} replicate_on_home_shard: partition moved", ntp);
        // Partition moved away after the shard lookup, this can happen
        // due to a race condition.
        co_return std::unexpected(ctp_stm_api_errc::failure);
    }
    auto stm = partition->raft()->stm_manager()->get<ctp_stm>();
    if (!stm) {
        vlog(cd_log.error, "{} replicate_on_home_shard: not a CTP", ntp);
        // Not a cloud-topic partition
        co_return std::unexpected(ctp_stm_api_errc::failure);
    }
    ctp_stm_api api(stm);
    co_return co_await replicate_with_retries(ntp, api, new_floor);
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::replicate_with_retries(
  model::ntp ntp, ctp_stm_api& api, kafka::offset new_floor) {
    auto last_error = ctp_stm_api_errc::timeout;
    for (int attempt = 0; attempt < max_attempts && !_as.abort_requested();
         ++attempt) {
        auto res = co_await api.set_min_allowed_local_threshold(
          new_floor, model::timeout_clock::now() + replicate_timeout, _as);
        if (res.has_value()) {
            co_return std::expected<void, ctp_stm_api_errc>{};
        }
        last_error = res.error();
        if (
          last_error == ctp_stm_api_errc::shutdown
          || last_error == ctp_stm_api_errc::not_leader) {
            vlog(
              cd_log.debug,
              "{} replicate_with_retries {} error: {}",
              ntp,
              last_error,
              last_error);
            // The stm is shutting down; retrying will not help.
            co_return std::unexpected(last_error);
        }
        vlog(
          cd_log.debug,
          "{} replicate_with_retries attempt {} failed: {}",
          ntp,
          attempt,
          last_error);
        if (attempt + 1 < max_attempts) {
            try {
                co_await ss::sleep_abortable<ss::lowres_clock>(
                  _retry_backoff, _as);
            } catch (const ss::sleep_aborted&) {
                co_return std::unexpected(ctp_stm_api_errc::shutdown);
            }
        }
    }
    vlog(
      cd_log.warn,
      "{} replicate_with_retries giving up after {} attempts, last error: {}",
      ntp,
      max_attempts,
      last_error);
    co_return std::unexpected(last_error);
}

} // namespace cloud_topics
