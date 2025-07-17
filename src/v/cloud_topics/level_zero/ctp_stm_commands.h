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

#include "cloud_topics/dl_version.h"
#include "serde/envelope.h"

namespace experimental::cloud_topics {

struct start_snapshot_cmd
  : public serde::envelope<
      start_snapshot_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    start_snapshot_cmd() noexcept = default;

    auto serde_fields() { return std::tie(); }
};

struct remove_snapshots_before_version_cmd
  : public serde::envelope<
      remove_snapshots_before_version_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    remove_snapshots_before_version_cmd() noexcept = default;
    explicit remove_snapshots_before_version_cmd(
      dl_version last_version_to_keep)
      : last_version_to_keep(last_version_to_keep) {}

    auto serde_fields() { return std::tie(last_version_to_keep); }

    dl_version last_version_to_keep{};
};

} // namespace experimental::cloud_topics
