/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/topic_configuration.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

namespace cloud_storage {

// A serde-friendly representation of topic_manifest.
struct topic_manifest_state
  : public serde::envelope<
      topic_manifest_state,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(cfg, initial_revision); }
    bool operator==(const topic_manifest_state&) const = default;

    cluster::topic_configuration cfg;
    model::initial_revision_id initial_revision;
};

} // namespace cloud_storage
