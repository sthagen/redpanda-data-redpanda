/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/metastore.h"

#include "cloud_topics/level_one/domain/domain_manager.h"
#include "cluster/shard_table.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace admin {

namespace {

[[noreturn]] void check_errc(cloud_topics::l1::metastore::errc ec) {
    switch (ec) {
    case cloud_topics::l1::metastore::errc::missing_ntp:
        throw serde::pb::rpc::not_found_exception("missing ntp");
    case cloud_topics::l1::metastore::errc::invalid_request:
        throw serde::pb::rpc::invalid_argument_exception();
    case cloud_topics::l1::metastore::errc::out_of_range:
        throw serde::pb::rpc::out_of_range_exception();
    case cloud_topics::l1::metastore::errc::transport_error:
        throw serde::pb::rpc::unavailable_exception("transport error");
    }
    throw serde::pb::rpc::unknown_exception();
}

} // namespace

seastar::future<proto::admin::metastore::get_offsets_response>
metastore_service_impl::get_offsets(
  serde::pb::rpc::context, proto::admin::metastore::get_offsets_request req) {
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});
    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }
    auto topic_id = topic_metadata->get().get_configuration().tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }
    proto::admin::metastore::get_offsets_response response;
    auto result = co_await _metastore->local().get_offsets(
      {*topic_id, model::partition_id{req.get_partition().get_partition()}});
    if (!result) {
        check_errc(result.error());
    }
    proto::admin::metastore::offsets offsets;
    offsets.set_start_offset(result.value().start_offset());
    offsets.set_next_offset(result.value().next_offset());
    response.set_offsets(std::move(offsets));
    co_return response;
}

seastar::future<proto::admin::metastore::get_size_response>
metastore_service_impl::get_size(
  serde::pb::rpc::context, proto::admin::metastore::get_size_request req) {
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});
    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }
    auto topic_id = topic_metadata->get().get_configuration().tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }
    proto::admin::metastore::get_size_response response;
    auto result = co_await _metastore->local().get_size(
      {*topic_id, model::partition_id{req.get_partition().get_partition()}});
    if (!result) {
        check_errc(result.error());
    }
    response.set_size_bytes(result.value().size);
    co_return response;
}

seastar::future<proto::admin::metastore::get_database_stats_response>
metastore_service_impl::get_database_stats(
  serde::pb::rpc::context ctx,
  proto::admin::metastore::get_database_stats_request req) {
    model::ntp metastore_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{
        static_cast<model::partition_id::type>(req.get_metastore_partition())}};

    // If we're not leader, reroute.
    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .get_database_stats(std::move(ctx), std::move(req));
    }

    // We're the leader; process locally.
    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }
    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp](this auto, cloud_topics::l1::domain_supervisor& sup)
        -> ss::future<std::expected<
          cloud_topics::l1::database_stats,
          cloud_topics::l1::rpc::errc>> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              co_return std::unexpected(
                cloud_topics::l1::rpc::errc::not_leader);
          }
          co_return co_await dm->get_database_stats();
      });
    if (!result_exp.has_value()) {
        switch (result_exp.error()) {
        case cloud_topics::l1::rpc::errc::not_leader:
            throw serde::pb::rpc::unavailable_exception("not leader");
        case cloud_topics::l1::rpc::errc::missing_ntp:
            throw serde::pb::rpc::not_found_exception("missing ntp");
        case cloud_topics::l1::rpc::errc::timed_out:
            throw serde::pb::rpc::deadline_exceeded_exception();
        default:
            throw serde::pb::rpc::unavailable_exception(
              fmt::format("error: {}", result_exp.error()));
        }
    }

    auto& result = result_exp.value();
    proto::admin::metastore::get_database_stats_response response;
    response.set_active_memtable_bytes(result.active_memtable_bytes);
    response.set_immutable_memtable_bytes(result.immutable_memtable_bytes);
    response.set_total_size_bytes(result.total_size_bytes);
    for (const auto& level : result.levels) {
        proto::admin::metastore::lsm_level level_proto;
        level_proto.set_level_number(level.level_number);

        for (const auto& file : level.files) {
            proto::admin::metastore::lsm_file file_proto;
            file_proto.set_epoch(file.epoch);
            file_proto.set_id(file.id);
            file_proto.set_size_bytes(file.size_bytes);
            file_proto.set_smallest_key_info(file.smallest_key_info);
            file_proto.set_largest_key_info(file.largest_key_info);
            level_proto.get_files().push_back(std::move(file_proto));
        }

        response.get_levels().push_back(std::move(level_proto));
    }

    co_return response;
}

} // namespace admin
