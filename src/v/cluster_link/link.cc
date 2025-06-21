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

#include "cluster_link/link.h"

#include "cluster_link/logger.h"

namespace cluster_link {

link::link(model::metadata config)
  : _config(std::move(config)) {}

ss::future<> link::start() {
    vlog(cllog.info, "Starting panda link {} ({})", _config.name, _config.uuid);
    return ss::now();
}

ss::future<> link::stop() {
    vlog(cllog.info, "Stopping panda link {} ({})", _config.name, _config.uuid);
    return ss::now();
}

void link::update_config(model::metadata config) {
    vlog(
      cllog.debug,
      "Updating panda link {} ({}): {}",
      _config.name,
      _config.uuid,
      config);
    _config = std::move(config);
}

const model::metadata& link::config() const { return _config; }
} // namespace cluster_link
