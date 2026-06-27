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

#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "rpc/errc.h"

namespace cloud_topics::notifier_detail {

/// Map an RPC transport error to the notifier's error code.
ctp_stm_api_errc map_transport_error(::rpc::errc ec);

} // namespace cloud_topics::notifier_detail
