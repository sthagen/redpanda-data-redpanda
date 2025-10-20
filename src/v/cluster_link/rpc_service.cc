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

#include "cluster_link/rpc_service.h"

#include "cluster_link/service.h"

namespace cluster_link::rpc {

ss::future<shadow_topic_report_response> service_impl::shadow_topic_report(
  shadow_topic_report_request request, ::rpc::streaming_context&) {
    return _svc.local().node_local_shadow_topic_report(std::move(request));
}

ss::future<shadow_link_status_report_response> service_impl::shadow_link_report(
  shadow_link_status_report_request request, ::rpc::streaming_context&) {
    return _svc.local().node_local_shadow_link_report(std::move(request));
}

}; // namespace cluster_link::rpc
