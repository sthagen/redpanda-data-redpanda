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

#pragma once

#include "cluster/fwd.h"
#include "proto/redpanda/core/admin/v2/features.proto.h"
#include "redpanda/admin/proxy/client.h"

namespace admin {

class features_service_impl : public proto::admin::features_service {
public:
    features_service_impl(
      admin::proxy::client,
      cluster::controller* controller,
      ss::sharded<cluster::metadata_cache>& md_cache);

    ss::future<proto::admin::finalize_upgrade_response> finalize_upgrade(
      serde::pb::rpc::context, proto::admin::finalize_upgrade_request) override;

    ss::future<proto::admin::get_upgrade_status_response> get_upgrade_status(
      serde::pb::rpc::context,
      proto::admin::get_upgrade_status_request) override;

private:
    admin::proxy::client _proxy_client;
    cluster::controller* _controller;
    ss::sharded<cluster::metadata_cache>& _md_cache;
};

} // namespace admin
