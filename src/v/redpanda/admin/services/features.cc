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

#include "redpanda/admin/services/features.h"

#include "base/vassert.h"
#include "cluster/controller.h"
#include "cluster/feature_manager.h"
#include "cluster/metadata_cache.h"
#include "config/configuration.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger featureslog{"admin_api_server/features_service"};

} // namespace

features_service_impl::features_service_impl(
  admin::proxy::client client,
  cluster::controller* controller,
  ss::sharded<cluster::metadata_cache>& md_cache)
  : _proxy_client(std::move(client))
  , _controller(controller)
  , _md_cache(md_cache) {}

ss::future<proto::admin::finalize_upgrade_response>
features_service_impl::finalize_upgrade(
  serde::pb::rpc::context ctx, proto::admin::finalize_upgrade_request req) {
    vlog(featureslog.trace, "finalize_upgrade: {}", req);

    if (config::shard_local_cfg().features_auto_finalization()) {
        throw serde::pb::rpc::failed_precondition_exception(
          "finalize_upgrade is only valid when features_auto_finalization "
          "is disabled");
    }

    const auto redirect_node = utils::redirect_to_leader(
      _md_cache.local(), model::controller_ntp, _proxy_client.self_node_id());

    if (redirect_node) {
        vlog(
          featureslog.debug,
          "Redirecting to leader of {}: {}",
          model::controller_ntp,
          *redirect_node);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::features_service_client>(
            *redirect_node)
          .finalize_upgrade(ctx, std::move(req));
    }

    auto& fm = _controller->get_feature_manager();
    auto status = co_await fm.invoke_on(
      cluster::feature_manager::backend_shard,
      [](cluster::feature_manager& fm) {
          return fm.submit_manual_finalize_request();
      });

    using finalize_status = cluster::feature_manager::finalize_status;
    switch (status) {
    case finalize_status::ok:
        co_return proto::admin::finalize_upgrade_response{};
    case finalize_status::not_leader:
        throw serde::pb::rpc::unavailable_exception(
          "controller leadership changed during processing; retry the "
          "request");
    }
    vunreachable("unhandled finalize_status: {}", static_cast<int>(status));
}

} // namespace admin
