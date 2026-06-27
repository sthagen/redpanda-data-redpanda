/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_zero/rpc/rpc_service.h"
#include "cloud_topics/level_zero/rpc/rpc_types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cloud_topics {
class level_zero_notifier;
}

namespace cloud_topics::l0::rpc {

class service final : public impl::l0_rpc_service {
public:
    service(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<level_zero_notifier>*);

    ss::future<set_min_allowed_local_threshold_reply>
    set_min_allowed_local_threshold(
      set_min_allowed_local_threshold_request,
      ::rpc::streaming_context&) override;

private:
    ss::sharded<level_zero_notifier>* _notifier;
};

} // namespace cloud_topics::l0::rpc
