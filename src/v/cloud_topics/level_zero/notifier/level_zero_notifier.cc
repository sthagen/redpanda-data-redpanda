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

#include "cloud_topics/level_zero/notifier/notifier_routing.h"
#include "cloud_topics/level_zero/rpc/rpc_service.h"
#include "cloud_topics/level_zero/rpc/rpc_types.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace cloud_topics {

namespace {
constexpr auto replicate_timeout = std::chrono::seconds{30};
} // namespace

namespace notifier_detail {

ctp_stm_api_errc map_transport_error(::rpc::errc ec) {
    return ec == ::rpc::errc::client_request_timeout
             ? ctp_stm_api_errc::timeout
             : ctp_stm_api_errc::failure;
}

} // namespace notifier_detail

level_zero_notifier::level_zero_notifier(
  model::node_id self,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::metadata_cache>* metadata,
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<rpc::connection_cache>* connections,
  std::chrono::milliseconds retry_backoff)
  : _self(self)
  , _leaders(leaders)
  , _metadata(metadata)
  , _shard_table(shard_table)
  , _partition_manager(partition_manager)
  , _connections(connections)
  , _retry_backoff(retry_backoff) {}

std::optional<model::ntp>
level_zero_notifier::resolve_ntp(const model::topic_id_partition& tidp) const {
    auto tns = _metadata->local().get_name_by_id(tidp.topic_id);
    if (!tns.has_value()) {
        return std::nullopt;
    }
    return model::ntp(tns->ns, tns->tp, tidp.partition);
}

ss::future<> level_zero_notifier::stop() {
    _as.request_abort();
    _inflight.broken();
    co_await _gate.close();
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::set_min_allowed_local_threshold(
  model::topic_id_partition tidp, kafka::offset new_floor) {
    auto ntp = resolve_ntp(tidp);
    if (!ntp.has_value()) {
        vlog(
          cd_log.warn,
          "{} set_min_allowed_local_threshold: could not resolve to an ntp "
          "(unknown topic id or stale metadata), new floor {}",
          tidp,
          new_floor);
        co_return std::unexpected(ctp_stm_api_errc::failure);
    }
    auto leader = _leaders->local().get_leader(*ntp);
    if (leader.has_value() && *leader == _self) {
        auto res = co_await replicate_locally(*ntp, new_floor);
        if (res.has_value() || res.error() != ctp_stm_api_errc::not_leader) {
            co_return res;
        }
    }
    // Leadership has moved away from this broker: forward the floor to the new
    // leader once it appears in cluster metadata.
    co_return co_await wait_for_leader_and_dispatch(
      std::move(*ntp), tidp, new_floor);
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::set_min_allowed_local_threshold_locally(
  model::topic_id_partition tidp, kafka::offset new_floor) {
    auto ntp = resolve_ntp(tidp);
    if (!ntp.has_value()) {
        vlog(
          cd_log.warn,
          "{} set_min_allowed_local_threshold_locally: could not resolve to an "
          "ntp (unknown topic id or stale metadata), new floor {}",
          tidp,
          new_floor);
        co_return std::unexpected(ctp_stm_api_errc::failure);
    }
    // No fallback: a broker that receives the RPC never forwards it again.
    co_return co_await replicate_locally(std::move(*ntp), new_floor);
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::wait_for_leader_and_dispatch(
  model::ntp ntp, model::topic_id_partition tidp, kafka::offset new_floor) {
    auto holder = _gate.hold();
    // Wait for the new leader to be elected and propagated into the metadata
    // cache, retrying the lookup max_attempts times.
    // Forward to the new leader.
    for (int attempt = 0; attempt < max_attempts && !_as.abort_requested();
         ++attempt) {
        auto leader = _leaders->local().get_leader(ntp);
        if (leader.has_value() && *leader != _self) {
            co_return co_await remote_dispatch(*leader, tidp, new_floor);
        } else if (leader.has_value() && *leader == _self) {
            co_return co_await replicate_locally(ntp, new_floor);
        }
        vlog(
          cd_log.debug,
          "{} wait_for_leader_and_dispatch: no new leader yet (attempt {}), "
          "new floor {}",
          ntp,
          attempt,
          new_floor);
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
      "{} wait_for_leader_and_dispatch: no new leader appeared after {} "
      "attempts, new "
      "floor {}",
      ntp,
      max_attempts,
      new_floor);
    co_return std::unexpected(ctp_stm_api_errc::not_leader);
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::replicate_locally(
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
level_zero_notifier::remote_dispatch(
  model::node_id leader,
  model::topic_id_partition tidp,
  kafka::offset new_floor) {
    auto holder = _gate.hold();
    using proto_t = l0::rpc::impl::l0_rpc_client_protocol;
    l0::rpc::set_min_allowed_local_threshold_request req{
      .tidp = tidp, .new_floor = new_floor};
    vlog(
      cd_log.debug,
      "{} forwarding new floor {} to leader {}",
      req.tidp,
      new_floor,
      leader);
    auto res = co_await _connections->local()
                 .with_node_client<proto_t>(
                   _self,
                   ss::this_shard_id(),
                   leader,
                   replicate_timeout,
                   [req](proto_t proto) mutable {
                       return proto.set_min_allowed_local_threshold(
                         std::move(req),
                         ::rpc::client_opts{
                           model::timeout_clock::now() + replicate_timeout});
                   })
                 .then(&::rpc::get_ctx_data<
                       l0::rpc::set_min_allowed_local_threshold_reply>);
    if (res.has_error()) {
        vlog(
          cd_log.warn,
          "failed forwarding new floor {} to leader {}: {}",
          new_floor,
          leader,
          res.error().message());
        co_return std::unexpected(
          notifier_detail::map_transport_error(
            static_cast<::rpc::errc>(res.error().value())));
    }
    const auto& reply = res.value();
    if (reply.ok) {
        co_return std::expected<void, ctp_stm_api_errc>{};
    }
    co_return std::unexpected(reply.ec);
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::replicate_on_home_shard(
  model::ntp ntp, kafka::offset new_floor) {
    auto holder = _gate.hold();
    auto units = co_await ss::get_units(_inflight, 1);

    auto partition = _partition_manager->local().get(ntp);
    if (!partition) {
        vlog(cd_log.info, "{} replicate_on_home_shard: partition moved", ntp);
        // Partition moved away after the shard lookup (a race). Report it as
        // not_leader so the caller forwards to the new location.
        co_return std::unexpected(ctp_stm_api_errc::not_leader);
    }
    auto stm = partition->raft()->stm_manager()->get<ctp_stm>();
    if (!stm) {
        vlog(cd_log.error, "{} replicate_on_home_shard: not a CTP", ntp);
        // Not a cloud-topic partition
        co_return std::unexpected(ctp_stm_api_errc::failure);
    }
    ctp_stm_api api(stm);
    co_return co_await replicate(ntp, api, new_floor);
}

ss::future<std::expected<void, ctp_stm_api_errc>>
level_zero_notifier::replicate(
  model::ntp ntp, ctp_stm_api& api, kafka::offset new_floor) {
    auto res = co_await api.set_min_allowed_local_threshold(
      new_floor, model::timeout_clock::now() + replicate_timeout, _as);
    if (!res.has_value()) {
        vlog(
          cd_log.debug,
          "{} replicate failed: {}, new floor {}",
          ntp,
          res.error(),
          new_floor);
        co_return std::unexpected(res.error());
    }
    co_return std::expected<void, ctp_stm_api_errc>{};
}

} // namespace cloud_topics
