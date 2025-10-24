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

#pragma once

#include "cluster/controller.h"
#include "proto/redpanda/core/admin/internal/v1/breakglass.proto.h"

#include <seastar/core/distributed.hh>

namespace admin::internal {

class breakglass_service_impl
  : public proto::admin::internal::breakglass_service {
public:
    explicit breakglass_service_impl(cluster::controller* controller) noexcept
      : _controller{controller} {}

    seastar::future<
      proto::admin::internal::controller_forced_reconfiguration_response>
      controller_forced_reconfiguration(
        serde::pb::rpc::context,
        proto::admin::internal::controller_forced_reconfiguration_request)
        override;

private:
    cluster::controller* _controller;
};

} // namespace admin::internal
