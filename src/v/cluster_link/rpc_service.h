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

#include "cluster_link/shadow_linking_rpc_service.h"

namespace cluster_link {
class service;
}

namespace cluster_link::rpc {

class service_impl : public shadow_linking_rpc_service {
public:
    service_impl(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<cluster_link::service>& service)
      : shadow_linking_rpc_service(sc, ssg)
      , _svc(service) {}

    ss::future<shadow_topic_report_response> shadow_topic_report(
      shadow_topic_report_request request, ::rpc::streaming_context&) override;

    ss::future<shadow_link_status_report_response> shadow_link_report(
      shadow_link_status_report_request request,
      ::rpc::streaming_context&) final;

private:
    ss::sharded<cluster_link::service>& _svc;
};
} // namespace cluster_link::rpc
