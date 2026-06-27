/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/rpc/service.h"

#include "cloud_topics/level_zero/notifier/level_zero_notifier.h"

namespace cloud_topics::l0::rpc {

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group smp_sg,
  ss::sharded<level_zero_notifier>* notifier)
  : impl::l0_rpc_service(sg, smp_sg)
  , _notifier(notifier) {}

ss::future<set_min_allowed_local_threshold_reply>
service::set_min_allowed_local_threshold(
  set_min_allowed_local_threshold_request req, ::rpc::streaming_context&) {
    // The handler runs on the leader node. Use the local-only path so a node
    // that receives this RPC never forwards again.
    auto res
      = co_await _notifier->local().set_min_allowed_local_threshold_locally(
        req.tidp, req.new_floor);
    set_min_allowed_local_threshold_reply reply;
    if (res.has_value()) {
        reply.ok = true;
    } else {
        reply.ec = res.error();
    }
    co_return reply;
}

} // namespace cloud_topics::l0::rpc
