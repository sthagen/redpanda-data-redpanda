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

#include "base/seastarx.h"
#include "cloud_topics/data_plane_api.h"

namespace experimental::cloud_topics {

// Encapsulates the required bits to access topic state from cloud topics,
// with minimal dependencies. This allows it to be passed around through
// different layers without introducing circular dependencies.
//
// NOTE: the seastar::sharded wants to know the size of the object at compile
// time. Simple container to use with seastar::sharded.
class state_accessors {
public:
    explicit state_accessors(ss::shared_ptr<data_plane_api> data_plane)
      : data_plane(std::move(data_plane)) {}
    ss::future<> start() { return data_plane->start(); }
    ss::future<> stop() { return data_plane->stop(); }
    ss::shared_ptr<data_plane_api> get_data_plane() { return data_plane; }

private:
    ss::shared_ptr<data_plane_api> data_plane;
};
} // namespace experimental::cloud_topics
