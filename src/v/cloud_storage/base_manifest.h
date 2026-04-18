/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/iostream.hh>

namespace cloud_storage {
struct serialized_data_stream {
    ss::input_stream<char> stream;
    size_t size_bytes;
};

enum class manifest_type {
    topic,
    partition,
    tx_range,
    cluster_metadata,
    spillover,
    topic_mount
};

inline fmt::iterator format_to(manifest_type t, fmt::iterator out) {
    switch (t) {
    case manifest_type::topic:
        return fmt::format_to(out, "topic");
    case manifest_type::partition:
        return fmt::format_to(out, "partition");
    case manifest_type::tx_range:
        return fmt::format_to(out, "tx-range");
    case manifest_type::cluster_metadata:
        return fmt::format_to(out, "cluster-metadata");
    case manifest_type::spillover:
        return fmt::format_to(out, "spillover");
    case manifest_type::topic_mount:
        return fmt::format_to(out, "topic_mount");
    }
}

enum class manifest_format {
    json,
    serde,
};

inline fmt::iterator format_to(manifest_format f, fmt::iterator out) {
    switch (f) {
    case manifest_format::json:
        return fmt::format_to(out, "json");
    case manifest_format::serde:
        return fmt::format_to(out, "serde");
    }
}
class base_manifest {
public:
    virtual ~base_manifest();

    /// Update manifest file from input_stream (remote set)
    virtual ss::future<> update(ss::input_stream<char> is) = 0;

    /// default implementation for derived classes that don't support multiple
    /// formats
    virtual ss::future<> update(manifest_format, ss::input_stream<char> is) {
        return update(std::move(is));
    }

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    virtual ss::future<serialized_data_stream> serialize() const;
    virtual ss::future<iobuf> serialize_buf() const = 0;

    /// Get manifest type
    virtual manifest_type get_manifest_type() const = 0;
};
} // namespace cloud_storage
