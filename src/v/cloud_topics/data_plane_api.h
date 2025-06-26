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

#include "base/outcome.h"
#include "cloud_topics/extent_meta.h"
#include "container/chunked_circular_buffer.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

namespace experimental::cloud_topics {

/// Dataplane API
class data_plane_api {
public:
    data_plane_api() = default;

    data_plane_api(const data_plane_api&) = delete;
    data_plane_api& operator=(const data_plane_api&) = delete;
    data_plane_api(data_plane_api&&) noexcept = delete;
    data_plane_api& operator=(data_plane_api&&) noexcept = delete;
    virtual ~data_plane_api() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    /// Write data batches and get back placeholder batches
    virtual ss::future<result<chunked_vector<extent_meta>>> write_and_debounce(
      model::ntp ntp,
      chunked_vector<model::record_batch> batches,
      std::chrono::milliseconds timeout)
      = 0;

    virtual ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp ntp,
      size_t output_size_estimate,
      chunked_vector<extent_meta> metadata,
      std::chrono::milliseconds timeout)
      = 0;
};

} // namespace experimental::cloud_topics
