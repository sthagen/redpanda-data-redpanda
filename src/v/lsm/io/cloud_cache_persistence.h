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

#include "cloud_io/cache_service.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "config/property.h"
#include "lsm/io/persistence.h"

namespace lsm::io {

/// Open a data persistence backed by the cloud cache and cloud storage.
/// SST files are read from object storage in chunks of `sst_chunk_size`
/// bytes, hydrated into the cache on demand; the binding is read per open so
/// the size can change at runtime. `gid` tags every cloud operation this
/// persistence issues for cloud_io admission control; defaults to
/// default_group.
ss::future<std::unique_ptr<data_persistence>> open_cloud_cache_data_persistence(
  cloud_io::cache* cache,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix,
  config::binding<size_t> sst_chunk_size,
  cloud_io::group_id gid = cloud_io::group_id::default_group);

/// Open a metadata persistence backed by cloud storage. `gid` tags every
/// cloud operation this persistence issues for cloud_io admission control;
/// defaults to default_group.
ss::future<std::unique_ptr<metadata_persistence>>
open_cloud_metadata_persistence(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix,
  cloud_io::group_id gid = cloud_io::group_id::default_group);

} // namespace lsm::io
