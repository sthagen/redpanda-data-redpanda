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

#include "absl/container/btree_set.h"
#include "base/seastarx.h"
#include "cloud_topics/types.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/set.h"

#include <seastar/core/future.hh>

#include <expected>

namespace experimental::cloud_topics::l1 {

// Represents the state managed by the replicated state machine that serves L1
// metadata.

// A description of a portion of an object that points at data for a contiguous
// range of batches belonging to a single partition. The object itself may
// contain ranges of data from many partitions.
struct extent
  : public serde::
      envelope<extent, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const extent&, const extent&) = default;
    std::strong_ordering operator<=>(const extent&) const = default;
    auto serde_fields() {
        return std::tie(
          base_offset, last_offset, max_timestamp, filepos, len, oid);
    }

    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp max_timestamp;
    size_t filepos;
    size_t len;
    // TODO: avoid duplicating the UUIDs with some indirection.
    object_id oid;
};

// State tracked per Kafka partition. The extents added to this state must have
// no overlaps and no gaps in order to ensure there is no data loss.
struct partition_state
  : public serde::
      envelope<partition_state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const partition_state&, const partition_state&)
      = default;
    auto serde_fields() { return std::tie(extents, start_offset, next_offset); }

    // The list of extents that comprise this partition. The ordering here
    // allows us to perform efficient lookups by offset.
    // Using a set here allows us to:
    // - perform efficient lookups by offset
    // - remove elements from the begining e.g. for prefix truncation
    // - replace ranges in the middle with new extents e.g. for compaction
    absl::btree_set<extent> extents;

    // The start offset of the partition. This may not align with the front of
    // `extents` if the offset was set through the Kafka API.
    kafka::offset start_offset{0};

    // The next offset expected to be added to this partition. In general this
    // should align with the back of `extents`, but is required when `extents`
    // is empty.
    kafka::offset next_offset{0};

    // TODO: strawman compaction:
    // - cleaned_range ([kafka::offset, kafka::offset]): range of offsets whose
    //   keys have been deduplicated fully. May not start at the beginning of
    //   the log in case the sliding window was too large to fit in memory.
    // - If there is no cleaned range, or if the cleaned range starts at or
    //   below the start offset, build sliding window from log end.
    // - If the clenaed range is above the start offset, building sliding
    //   window below the start of the cleaned range
    // - When adding objects from compaction, expand the cleaned range.
};

// Tracks the state managed for each partition of a Kafka topic.
struct topic_state
  : public serde::
      envelope<topic_state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const topic_state&, const topic_state&) = default;
    auto serde_fields() { return std::tie(pid_to_state); }
    chunked_hash_map<model::partition_id, partition_state> pid_to_state;

    topic_state copy() const;
};

// Metadata about a given object that is not specific to any partition.
struct object_entry
  : public serde::
      envelope<object_entry, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const object_entry&, const object_entry&) = default;
    auto serde_fields() {
        return std::tie(total_data_size, removed_data_size, footer_pos);
    }
    size_t total_data_size{0};
    size_t removed_data_size{0};
    size_t footer_pos{0};
};

// Tracks the state of each topic revision.
struct state
  : public serde::envelope<state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const state&, const state&) = default;
    auto serde_fields() { return std::tie(topic_to_state, objects); }
    chunked_hash_map<model::topic_id, topic_state> topic_to_state;
    chunked_hash_map<object_id, object_entry> objects;

    state copy() const;

    std::optional<std::reference_wrapper<const partition_state>>
    partition_state(const model::topic_id_partition&) const;
};

} // namespace experimental::cloud_topics::l1
