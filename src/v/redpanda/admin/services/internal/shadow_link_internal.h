/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/fwd.h"
#include "cluster_link/fwd.h"
#include "model/fundamental.h"
#include "proto/redpanda/core/admin/internal/shadow_link_internal/v1/shadow_link_internal.proto.h"
#include "redpanda/admin/proxy/client.h"

namespace admin::internal {
// Implementation of the internal Shadow Link escape hatch service
class shadow_link_internal_service_impl
  : public proto::admin::internal::shadow_link::shadow_link_internal_service {
public:
    shadow_link_internal_service_impl(
      admin::proxy::client proxy_client,
      ss::sharded<cluster_link::service>* service,
      ss::sharded<cluster::metadata_cache>* md_cache);

    // Removes Shadow Topic from a Shadow Link, stopping all replication and
    // property syncing.  Will not remove the topic from the Shadow Cluster
    ss::future<
      proto::admin::internal::shadow_link::remove_shadow_topic_response>
      remove_shadow_topic(
        serde::pb::rpc::context,
        proto::admin::internal::shadow_link::remove_shadow_topic_request) final;

    // Force changes the state of a Shadow Topic
    ss::future<proto::admin::internal::shadow_link::
                 force_update_shadow_topic_state_response>
      force_update_shadow_topic_state(
        serde::pb::rpc::context,
        proto::admin::internal::shadow_link::
          force_update_shadow_topic_state_request) final;

private:
    std::optional<model::node_id> redirect_to(const model::ntp& ntp);

private:
    admin::proxy::client _proxy_client;
    ss::sharded<cluster_link::service>* _service;
    ss::sharded<cluster::metadata_cache>* _md_cache;
};
} // namespace admin::internal
