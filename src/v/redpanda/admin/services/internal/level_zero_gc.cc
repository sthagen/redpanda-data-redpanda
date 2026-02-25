/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/level_zero_gc.h"

#include "base/vassert.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cloud_topics/state_accessors.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "serde/protobuf/rpc.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shard_id.hh>

namespace {
ss::logger gclog("level_zero_gc_service");
constexpr model::timeout_clock::duration leader_timeout = 5s;
constexpr model::timeout_clock::duration advance_epoch_timeout = 10s;
void validate_initialized(ss::sharded<cloud_topics::level_zero_gc>* gc) {
    if (!gc->local_is_initialized()) {
        throw serde::pb::rpc::unavailable_exception(
          "Cloud topics level zero GC not initialized");
    }
}
} // namespace

namespace admin {

namespace {
constexpr proto::admin::level_zero_gc::status
map_gc_state(cloud_topics::level_zero_gc::state st) {
    using namespace proto::admin::level_zero_gc;
    switch (st) {
        using enum cloud_topics::level_zero_gc::state;
    case paused:
        return status::l0_gc_status_paused;
    case running:
        return status::l0_gc_status_running;
    case stopping:
        return status::l0_gc_status_stopping;
    case stopped:
        return status::l0_gc_status_stopped;
    }
    vunreachable("Unrecognized level_zero_gc::state {}", st);
}
} // namespace

level_zero_gc_service_impl::level_zero_gc_service_impl(
  model::node_id self,
  admin::proxy::client pc,
  ss::sharded<cloud_topics::level_zero_gc>* gc,
  ss::sharded<cluster::members_table>* mt,
  ss::sharded<cluster::partition_manager>* pm,
  ss::sharded<cluster::partition_leaders_table>* pl,
  ss::sharded<cluster::shard_table>* st)
  : _self(self)
  , _proxy_client(std::move(pc))
  , _gc(gc)
  , _members_table(mt)
  , _partition_manager(pm)
  , _partition_leaders(pl)
  , _shard_table(st) {}

seastar::future<proto::admin::level_zero_gc::get_status_response>
level_zero_gc_service_impl::get_status(
  serde::pb::rpc::context ctx,
  proto::admin::level_zero_gc::get_status_request req) {
    validate_initialized(_gc);
    using namespace proto::admin::level_zero_gc;
    auto [local, remote] = validate_request_routing(ctx, req);

    get_status_response response;
    if (local == apply_local::yes) {
        auto gc_states = co_await _gc->map(
          [](const cloud_topics::level_zero_gc& gc) { return gc.get_state(); });
        if (gc_states.size() != ss::smp::count) {
            throw serde::pb::rpc::internal_exception(
              "Status collection failed");
        }
        auto& node = response.get_nodes().emplace_back();
        node.set_node_id(_self);
        node.get_shards().reserve(gc_states.size());
        std::ranges::transform(
          std::views::zip(
            std::views::iota(0u, static_cast<unsigned>(gc_states.size())),
            gc_states),
          std::back_inserter(node.get_shards()),
          [](auto pr) {
              shard_status shard;
              shard.set_shard_id(std::get<0>(pr));
              shard.set_status(map_gc_state(std::get<1>(pr)));
              return shard;
          });
    }

    if (remote == apply_remote::yes) {
        auto results = co_await dispatch_and_collect<
          get_status_request,
          get_status_response,
          node_status,
          level_zero_gc_service_client>(
          req,
          [ctx](
            level_zero_gc_service_client& cl,
            model::node_id id,
            const get_status_request&) {
              get_status_request proxy_req;
              proxy_req.set_node_id(id);
              return cl.get_status(ctx, std::move(proxy_req));
          },
          [](get_status_response rsp) { return std::move(rsp.get_nodes()); });
        response.get_nodes().reserve(
          response.get_nodes().size() + results.size());
        std::ranges::move(results, std::back_inserter(response.get_nodes()));
    }

    co_return response;
}

seastar::future<proto::admin::level_zero_gc::start_response>
level_zero_gc_service_impl::start(
  serde::pb::rpc::context ctx, proto::admin::level_zero_gc::start_request req) {
    validate_initialized(_gc);
    using namespace proto::admin::level_zero_gc;
    auto [local, remote] = validate_request_routing(ctx, req);

    start_response response;
    if (local == apply_local::yes) {
        co_await _gc->invoke_on_all(&cloud_topics::level_zero_gc::start);
        auto& result = response.get_results().emplace_back();
        result.set_node_id(_self);
    }

    if (remote == apply_remote::yes) {
        auto results = co_await dispatch_and_collect<
          start_request,
          start_response,
          start_result,
          level_zero_gc_service_client>(
          req,
          [ctx](
            level_zero_gc_service_client& cl,
            model::node_id id,
            const start_request&) {
              start_request proxy_req;
              proxy_req.set_node_id(id);
              return cl.start(ctx, std::move(proxy_req));
          },
          [](start_response rsp) { return std::move(rsp.get_results()); });
        response.get_results().reserve(
          response.get_results().size() + results.size());
        std::ranges::move(results, std::back_inserter(response.get_results()));
    }

    co_return response;
}

seastar::future<proto::admin::level_zero_gc::pause_response>
level_zero_gc_service_impl::pause(
  serde::pb::rpc::context ctx, proto::admin::level_zero_gc::pause_request req) {
    validate_initialized(_gc);
    using namespace proto::admin::level_zero_gc;
    auto [local, remote] = validate_request_routing(ctx, req);

    pause_response response;
    if (local == apply_local::yes) {
        co_await _gc->invoke_on_all(&cloud_topics::level_zero_gc::pause);
        auto& result = response.get_results().emplace_back();
        result.set_node_id(_self);
    }

    if (remote == apply_remote::yes) {
        auto results = co_await dispatch_and_collect<
          pause_request,
          pause_response,
          pause_result,
          level_zero_gc_service_client>(
          req,
          [ctx](
            level_zero_gc_service_client& cl,
            model::node_id id,
            const pause_request&) {
              pause_request proxy_req;
              proxy_req.set_node_id(id);
              return cl.pause(ctx, std::move(proxy_req));
          },
          [](pause_response rsp) { return std::move(rsp.get_results()); });
        response.get_results().reserve(
          response.get_results().size() + results.size());
        std::ranges::move(results, std::back_inserter(response.get_results()));
    }

    co_return response;
}

namespace {

std::unique_ptr<cloud_topics::frontend>
make_ct_frontend(cluster::partition_manager& pm, const model::ntp& ntp) {
    auto partition = pm.get(ntp);
    if (partition == nullptr) {
        throw serde::pb::rpc::not_found_exception(
          ssx::sformat("TopicPartition {} not found", ntp.tp));
    }
    if (!partition->get_ntp_config().cloud_topic_enabled()) {
        throw serde::pb::rpc::failed_precondition_exception(
          ssx::sformat("TopicPartition {} is not a cloud topic", ntp.tp));
    }
    auto ct_state = partition->get_cloud_topics_state();
    if (ct_state == nullptr || !ct_state->local_is_initialized()) {
        throw serde::pb::rpc::failed_precondition_exception(
          "Cloud topics subsystem is not initialized");
    }
    return std::make_unique<cloud_topics::frontend>(
      partition, ct_state->local().get_data_plane());
}

proto::admin::level_zero_gc::epoch_info
epoch_info_to_pb(cloud_topics::frontend::epoch_info info) {
    proto::admin::level_zero_gc::epoch_info result;
    result.set_estimated_inactive_epoch(info.estimated_inactive_epoch);
    result.set_max_applied_epoch(info.max_applied_epoch);
    result.set_last_reconciled_log_offset(info.last_reconciled_log_offset);
    result.set_current_epoch_window_offset(info.current_epoch_window_offset);
    return result;
}

seastar::future<proto::admin::level_zero_gc::advance_epoch_response>
do_advance_epoch(
  cluster::partition_manager& pm,
  const model::ntp& ntp,
  cloud_topics::cluster_epoch new_epoch) {
    using namespace proto::admin::level_zero_gc;
    auto fe = make_ct_frontend(pm, ntp);
    auto info = co_await fe->advance_epoch(
      new_epoch, model::timeout_clock::now() + advance_epoch_timeout);
    if (!info.has_value()) {
        using enum cloud_topics::frontend_errc;
        switch (info.error()) {
        case not_leader_for_partition:
            throw serde::pb::rpc::failed_precondition_exception(
              ssx::sformat("advance_epoch failed: {}", info.error()));
        case timeout:
            throw serde::pb::rpc::deadline_exceeded_exception(
              ssx::sformat("advance_epoch failed: {}", info.error()));
        default:
            throw serde::pb::rpc::internal_exception(
              ssx::sformat("advance_epoch failed: {}", info.error()));
        }
    }
    advance_epoch_response response;
    response.set_epoch(epoch_info_to_pb(info.value()));
    co_return response;
}

} // namespace

seastar::future<proto::admin::level_zero_gc::advance_epoch_response>
level_zero_gc_service_impl::advance_epoch(
  serde::pb::rpc::context ctx,
  proto::admin::level_zero_gc::advance_epoch_request req) {
    using namespace proto::admin::level_zero_gc;
    auto& tp = req.get_partition();
    auto ntp = model::ntp{
      model::kafka_namespace, tp.get_topic(), tp.get_partition()};

    auto leader = co_await _partition_leaders->local().wait_for_leader(
      ntp, model::timeout_clock::now() + leader_timeout, std::nullopt);

    if (leader != _self) {
        if (proxy::is_proxied(ctx)) {
            throw serde::pb::rpc::unavailable_exception("Not leader");
        }
        co_return co_await _proxy_client
          .make_client_for_node<level_zero_gc_service_client>(leader)
          .advance_epoch(ctx, std::move(req));
    }

    auto shard = _shard_table->local().shard_for(ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("Not leader");
    }

    co_return co_await _partition_manager->invoke_on(
      shard.value(),
      [ntp, new_epoch = req.get_new_epoch()](
        cluster::partition_manager& pm) -> ss::future<advance_epoch_response> {
          return do_advance_epoch(
            pm, ntp, cloud_topics::cluster_epoch{new_epoch});
      });
}

seastar::future<proto::admin::level_zero_gc::get_epoch_info_response>
level_zero_gc_service_impl::get_epoch_info(
  serde::pb::rpc::context ctx,
  proto::admin::level_zero_gc::get_epoch_info_request req) {
    using namespace proto::admin::level_zero_gc;

    auto& tp = req.get_partition();
    auto ntp = model::ntp{
      model::kafka_namespace, tp.get_topic(), tp.get_partition()};

    auto leader = _partition_leaders->local().get_leader(ntp);
    if (!leader.has_value()) {
        throw serde::pb::rpc::unavailable_exception(
          ssx::sformat("No leader for {}", ntp.tp));
    }

    if (leader.value() != _self) {
        if (proxy::is_proxied(ctx)) {
            throw serde::pb::rpc::unavailable_exception("Not leader");
        }
        co_return co_await _proxy_client
          .make_client_for_node<level_zero_gc_service_client>(leader.value())
          .get_epoch_info(ctx, std::move(req));
    }

    auto shard = _shard_table->local().shard_for(ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("Not leader");
    }

    auto info = co_await _partition_manager->invoke_on(
      shard.value(), [ntp](cluster::partition_manager& pm) {
          auto fe = make_ct_frontend(pm, ntp);
          return fe->get_epoch_info();
      });

    get_epoch_info_response response;
    response.set_epoch(epoch_info_to_pb(info));
    co_return response;
}

} // namespace admin
