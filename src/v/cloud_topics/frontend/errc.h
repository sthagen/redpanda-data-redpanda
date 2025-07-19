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

namespace experimental::cloud_topics {
// Frontend error codes
enum class frontend_errc {
    offset_not_available,
    invalid_topic_exception,
    not_leader_for_partition,
    offset_out_of_range,
};
} // namespace experimental::cloud_topics
