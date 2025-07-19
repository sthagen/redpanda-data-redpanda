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

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/level_one/domain/domain_supervisor.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

namespace experimental::cloud_topics {

// Simple container to use with seastar::sharded.
// The seastar::sharded wants to know the size of the object at compile time.
class app {
public:
    explicit app(
      ss::shared_ptr<data_plane_api>, std::unique_ptr<l1::domain_supervisor>);

    app(const app&) = delete;
    app& operator=(const app&) = delete;
    app(app&&) noexcept = delete;
    app& operator=(app&&) noexcept = delete;
    ~app() = default;

    seastar::future<> start();
    seastar::future<> stop();

    ss::shared_ptr<data_plane_api> get_data_plane_api();

    // TODO: add 'get_control_plane_api' etc

private:
    ss::shared_ptr<data_plane_api> _data_plane;
    std::unique_ptr<l1::domain_supervisor> _domain_supervisor;
};

} // namespace experimental::cloud_topics
