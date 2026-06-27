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
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"

namespace cloud_topics::l0::rpc {

struct set_min_allowed_local_threshold_reply
  : serde::envelope<
      set_min_allowed_local_threshold_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(ok, ec); }

    // Success is carried explicitly because the local path returns
    // std::expected<void, ...>: `ok` true means the floor was replicated; when
    // false, `ec` holds the failure reason.
    bool ok{false};
    ctp_stm_api_errc ec{ctp_stm_api_errc::failure};
};

struct set_min_allowed_local_threshold_request
  : serde::envelope<
      set_min_allowed_local_threshold_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = set_min_allowed_local_threshold_reply;
    auto serde_fields() { return std::tie(tidp, new_floor); }

    model::topic_id_partition tidp;
    kafka::offset new_floor;
};

} // namespace cloud_topics::l0::rpc
