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
#include "cloud_topics/app.h"

namespace experimental::cloud_topics {

app::app(
  ss::shared_ptr<data_plane_api> dp,
  std::unique_ptr<l1::domain_supervisor> l1_cp)
  : _data_plane(std::move(dp))
  , _domain_supervisor(std::move(l1_cp)) {}

seastar::future<> app::start() {
    co_await _domain_supervisor->start();
    co_await _data_plane->start();
}

seastar::future<> app::stop() {
    co_await _domain_supervisor->stop();
    co_await _data_plane->stop();
}

ss::shared_ptr<data_plane_api> app::get_data_plane_api() { return _data_plane; }

} // namespace experimental::cloud_topics
