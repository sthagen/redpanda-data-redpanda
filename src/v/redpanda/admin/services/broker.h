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

#include "proto/redpanda/core/admin/v2/broker.proto.h"
#include "redpanda/admin/proxy/client.h"

namespace admin {

// An admin service that provides some reflection capabilities
// for the admin API, such as the version of Redpanda as well as
// what requests are available.
class broker_service_impl : public proto::admin::broker_service {
public:
    broker_service_impl(
      admin::proxy::client,
      std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* services);

    ss::future<proto::admin::get_broker_response> get_broker(
      serde::pb::rpc::context, proto::admin::get_broker_request) override;
    ss::future<proto::admin::list_brokers_response> list_brokers(
      serde::pb::rpc::context, proto::admin::list_brokers_request) override;

private:
    proto::admin::broker self_broker() const;

    admin::proxy::client _proxy_client;
    std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* _services;
};

} // namespace admin
