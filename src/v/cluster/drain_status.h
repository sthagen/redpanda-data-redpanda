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
#include "base/format_to.h"
#include "serde/envelope.h"

#include <cstddef>
#include <optional>
#include <tuple>

namespace cluster {

/*
 * finished:     draining has completed
 * errors:       draining finished with errors
 * partitions:   total partitions
 * eligible:     total drain-eligible partitions
 * transferring: total partitions currently transferring
 * failed:       total transfers failed in last batch
 *
 * the optional fields may not be set if draining has been requested, but
 * not yet started. in this case the values are not yet known.
 */
struct drain_status
  : serde::envelope<drain_status, serde::version<0>, serde::compat_version<0>> {
    bool finished{false};
    bool errors{false};
    std::optional<size_t> partitions;
    std::optional<size_t> eligible;
    std::optional<size_t> transferring;
    std::optional<size_t> failed;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(const drain_status&, const drain_status&) = default;

    auto serde_fields() {
        return std::tie(
          finished, errors, partitions, eligible, transferring, failed);
    }
};

} // namespace cluster
