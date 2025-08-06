/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/app.h"

#include <seastar/core/coroutine.hh>

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

l1::domain_supervisor* app::get_l1_domain_supervisor() {
    return _domain_supervisor.get();
}

} // namespace experimental::cloud_topics
