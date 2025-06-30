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

#include "base/seastarx.h"
#include "cloud_topics/types.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

#include <expected>

namespace experimental::cloud_topics::l1 {

// Interface to interact with the L1 metastore. Meant to be totally agnostic to
// the underlying implementation, whether it's an in-memory store, replicated
// store, or even an external service.
//
// The metastore contains metadata about cloud topic partitions and the L1
// objects in which they store their data. It is designed to be a simple state
// store, performing atomic updates to state and answering simple queries for
// callers. The metastore itself isn't meant to perform complex partition
// storage management tasks (e.g. garbage collection or compaction), but rather
// it exposes primitives to enable robustly performing such tasks.
//
// At a high level, the metastore contains the following metadata:
// - a fixed set of information about each Kafka partition (e.g. start offset,
//   next offset),
// - the set of extents (pointers into L1 objects) associated with each Kafka
//   partition, with enough metadata to find an L1 object by offset or
//   timestamp,
// - the set of L1 objects and some information about them to enable efficient
//   object downloads (e.g. footer location).
//
// While callers should generally avoid sending invalid requests to the
// metastore (e.g. creating gaps in offset space), it is up to the metastore
// implementation to ensure such requests are rejected and don't have harmful
// side effects. As such, callers can think of this interface as thread safe.
class metastore {
public:
    enum class errc {
        missing_ntp,
        invalid_request,
        out_of_range,
    };
    struct object_metadata {
        struct ntp_metadata {
            model::topic_id_partition tidp;
            kafka::offset base_offset;
            kafka::offset last_offset;
            model::timestamp max_timestamp;
            first_byte_offset_t pos;
            byte_range_size_t size;
        };

        object_id oid;
        first_byte_offset_t footer_pos;
        chunked_vector<ntp_metadata> ntp_metas;
    };

    struct object_response {
        object_id oid;
        size_t footer_pos;
    };

    struct offsets_response {
        kafka::offset start_offset;
        kafka::offset next_offset;
    };

    // Returns offsets (e.g. start, next) for the given partition.
    virtual ss::future<std::expected<offsets_response, errc>>
    get_offsets(const model::topic_id_partition&) = 0;

    // Adds the given objects to the metastore. If any are invalid, e.g.
    // because they break an invariant of a partition's offset ranges, all
    // objects are rejected.
    virtual ss::future<std::expected<void, errc>>
    add_objects(const chunked_vector<object_metadata>&) = 0;

    // Finds the first object of a given partition with data greater than or
    // equal to the given offset. If no such offset exists, returns
    // `out_of_range`.
    virtual ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, kafka::offset) = 0;

    // Finds the first object of a given partition with data greater than or
    // equal to the given timestamp. If no such timestamp exists, returns
    // `out_of_range`.
    virtual ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition&, model::timestamp) = 0;
};

} // namespace experimental::cloud_topics::l1
