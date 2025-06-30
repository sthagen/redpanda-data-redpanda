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

#include "cluster_link/model/types.h"
#include "model/fundamental.h"
#include "model/record.h"

namespace cluster::cluster_link::testing {
model::record_batch
  create_upsert_command(model::offset, ::cluster_link::model::metadata);
model::record_batch create_remove_command(::cluster_link::model::name_t);
model::record_batch create_add_mirror_topic_command(
  ::cluster_link::model::id_t, ::cluster_link::model::add_mirror_topic_cmd);
model::record_batch create_update_mirror_topic_state_command(
  ::cluster_link::model::id_t,
  ::cluster_link::model::update_mirror_topic_state_cmd);

::cluster_link::model::mirror_topic_metadata create_mirror_topic_metadata(
  ::cluster_link::model::mirror_topic_state,
  ::model::topic source_topic_name,
  std::optional<::model::topic_id> source_topic_id = std::nullopt,
  std::optional<::model::topic_id> destination_topoic_id = std::nullopt);
} // namespace cluster::cluster_link::testing
