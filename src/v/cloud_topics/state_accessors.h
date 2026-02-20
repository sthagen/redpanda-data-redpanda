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

#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"

namespace cluster {
class metadata_cache;
}

namespace cloud_topics {

class data_plane_api;

namespace l1 {
class metastore;
class io;
} // namespace l1

// Encapsulates the required bits to access topic state from cloud topics,
// with minimal dependencies. This allows it to be passed around through
// different layers without introducing circular dependencies.
//
class state_accessors {
public:
    explicit state_accessors(
      data_plane_api* data_plane,
      l1::metastore* metastore,
      l1::io* io,
      cluster::metadata_cache* metadata_cache,
      level_one_reader_probe* l1_reader_probe)
      : data_plane(data_plane)
      , l1_metastore(metastore)
      , l1_io(io)
      , metadata_cache(metadata_cache)
      , l1_reader_probe(l1_reader_probe) {}

    data_plane_api* get_data_plane() { return data_plane; }
    l1::metastore* get_l1_metastore() { return l1_metastore; }
    l1::io* get_l1_io() { return l1_io; }
    level_one_reader_probe* get_l1_reader_probe() { return l1_reader_probe; }
    cluster::metadata_cache* get_metadata_cache() { return metadata_cache; }

private:
    data_plane_api* data_plane;
    l1::metastore* l1_metastore;
    l1::io* l1_io;
    cluster::metadata_cache* metadata_cache;
    level_one_reader_probe* l1_reader_probe;
};
} // namespace cloud_topics
