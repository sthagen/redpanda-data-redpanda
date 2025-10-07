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

#include "base/format_to.h"
#include "cluster/cluster_link/errc.h"
#include "model/fundamental.h"

namespace cluster::cluster_link {

struct topic_result
  : serde::envelope<topic_result, serde::version<0>, serde::compat_version<0>> {
    topic_result() noexcept = default;
    explicit topic_result(model::topic t, errc ec = errc::success)
      : topic(std::move(t))
      , ec(ec) {}
    ::model::topic topic;
    errc ec{errc::success};

    friend bool operator==(const topic_result&, const topic_result&) = default;

    fmt::iterator format_to(fmt::iterator it) const;

    auto serde_fields() { return std::tie(topic, ec); }
};

} // namespace cluster::cluster_link
