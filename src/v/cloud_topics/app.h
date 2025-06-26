/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cloud_topics/data_plane_api.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

namespace experimental::cloud_topics {

// Simple container to use with seastar::sharded.
// The seastar::sharded wants to know the size of the object at compile time.
class app {
public:
    explicit app(ss::shared_ptr<data_plane_api>);

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
    ss::shared_ptr<data_plane_api> _impl;
};

} // namespace experimental::cloud_topics
