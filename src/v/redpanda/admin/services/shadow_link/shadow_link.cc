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

#include "redpanda/admin/services/shadow_link/shadow_link.h"

#include "cluster_link/service.h"
#include "redpanda/admin/services/shadow_link/converter.h"
#include "serde/protobuf/rpc.h"

namespace admin {
ss::logger sllog("shadow_link_service");
namespace {

template<typename T>
T handle_error(cluster_link::result<T> result) {
    if (result.has_value()) {
        return std::move(result).assume_value();
    }
    auto info = result.assume_error();
    switch (info.code()) {
    case cluster_link::errc::success:
        vassert(false, "Unexpected success code in handle_error");
    case cluster_link::errc::invalid_task_state_change:
    case cluster_link::errc::task_not_running:
    case cluster_link::errc::task_already_running:
    case cluster_link::errc::failed_to_start_task:
    case cluster_link::errc::task_already_registered_on_link:
    case cluster_link::errc::task_creation_failed:
    case cluster_link::errc::rpc_error:
    case cluster_link::errc::link_creation_failed:
        throw serde::pb::rpc::internal_exception(info.message());
    case cluster_link::errc::failed_to_connect_to_remote_cluster:
    case cluster_link::errc::remote_cluster_does_not_support_required_api:
    case cluster_link::errc::link_connection_failed:
    case cluster_link::errc::service_shutting_down:
        throw serde::pb::rpc::unavailable_exception(info.message());
    case cluster_link::errc::cluster_link_disabled:
        throw serde::pb::rpc::failed_precondition_exception(info.message());
    case cluster_link::errc::link_id_not_found:
        throw serde::pb::rpc::not_found_exception(info.message());
    case cluster_link::errc::invalid_configuration:
        throw serde::pb::rpc::invalid_argument_exception(info.message());
    case cluster_link::errc::topic_already_mirrored:
    case cluster_link::errc::topic_mirrored_by_other_link:
    case cluster_link::errc::topic_not_being_mirrored:
        throw serde::pb::rpc::already_exists_exception(info.message());
    case cluster_link::errc::link_limit_reached:
        throw serde::pb::rpc::resource_exhausted_exception(info.message());
    }
}
} // namespace

shadow_link_service_impl::shadow_link_service_impl(
  ss::sharded<cluster_link::service>* service)
  : _service(service) {}

ss::future<proto::admin::create_shadow_link_response>
shadow_link_service_impl::create_shadow_link(
  serde::pb::rpc::context, proto::admin::create_shadow_link_request req) {
    vlog(sllog.trace, "create_shadow_link: {}", req);

    auto md = convert_create_to_metadata(std::move(req));
    auto get_resp = _service->local().get_cluster_link(md.name);
    if (get_resp.has_value()) {
        throw serde::pb::rpc::already_exists_exception(
          ssx::sformat("Shadow link with name {} already exists", md.name));
    }

    auto resp = handle_error(
      co_await _service->local().upsert_cluster_link(std::move(md)));

    proto::admin::create_shadow_link_response sl_resp;
    sl_resp.set_shadow_link(metadata_to_shadow_link(std::move(resp)));

    co_return sl_resp;
}

ss::future<proto::admin::delete_shadow_link_response>
shadow_link_service_impl::delete_shadow_link(
  serde::pb::rpc::context, proto::admin::delete_shadow_link_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

ss::future<proto::admin::get_shadow_link_response>
shadow_link_service_impl::get_shadow_link(
  serde::pb::rpc::context, proto::admin::get_shadow_link_request req) {
    vlog(sllog.trace, "get_shadow_link: {}", req);

    auto resp = handle_error(_service->local().get_cluster_link(
      cluster_link::model::name_t{req.get_name()}));
    proto::admin::get_shadow_link_response get_resp;
    get_resp.set_shadow_link(metadata_to_shadow_link(std::move(resp)));
    co_return get_resp;
}

ss::future<proto::admin::list_shadow_links_response>
shadow_link_service_impl::list_shadow_links(
  serde::pb::rpc::context, proto::admin::list_shadow_links_request req) {
    vlog(sllog.trace, "list_shadow_links: {}", req);

    auto resp = handle_error(_service->local().list_cluster_links());

    proto::admin::list_shadow_links_response list_resp;
    chunked_vector<proto::admin::shadow_link> links;
    links.reserve(resp.size());
    for (auto& md : resp) {
        links.emplace_back(metadata_to_shadow_link(std::move(md)));
    }

    list_resp.set_shadow_links(std::move(links));

    co_return list_resp;
}

ss::future<proto::admin::update_shadow_link_response>
shadow_link_service_impl::update_shadow_link(
  serde::pb::rpc::context, proto::admin::update_shadow_link_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

ss::future<proto::admin::fail_over_response>
shadow_link_service_impl::fail_over(
  serde::pb::rpc::context, proto::admin::fail_over_request) {
    throw serde::pb::rpc::unimplemented_exception();
}
} // namespace admin
