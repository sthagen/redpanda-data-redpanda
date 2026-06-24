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

#include "datalake/coordinator/frontend.h"
#include "proto/redpanda/core/admin/v2/iceberg.proto.h"

namespace admin {

// iceberg_service_impl is the public read-only surface over the Datalake
// subsystem. APIs implemented here are publicly available, in contrast to the
// `datalake` service, which is internal.
class iceberg_service_impl : public proto::admin::iceberg_service {
public:
    explicit iceberg_service_impl(
      ss::sharded<datalake::coordinator::frontend>* coordinator_fe);

    ss::future<proto::admin::get_iceberg_status_response> get_iceberg_status(
      serde::pb::rpc::context,
      proto::admin::get_iceberg_status_request) override;

private:
    ss::sharded<datalake::coordinator::frontend>* _coordinator_fe;
};

} // namespace admin
