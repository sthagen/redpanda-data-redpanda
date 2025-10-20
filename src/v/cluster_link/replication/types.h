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

#include "base/format_to.h"
#include "model/fundamental.h"

namespace cluster_link::replication {
struct partition_offsets_report {
    kafka::offset source_start_offset{-1};
    kafka::offset source_hwm{-1};
    kafka::offset source_lso{-1};
    ss::lowres_clock::time_point update_time{};
    kafka::offset shadow_hwm{-1};

    fmt::iterator format_to(fmt::iterator) const;
};
} // namespace cluster_link::replication
