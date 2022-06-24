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

#include "bytes/iobuf.h"
#include "hashing/xx.h"
#include "seastarx.h"
#include "serde/envelope.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>

#include <chrono>
#include <string_view>

namespace cloud_storage {

/// Access time tracker maintains map from filename hash to
/// the timestamp that represents the time when the file was
/// accessed last.
///
/// It is possible to have conflicts. In case of conflict
/// 'add_timestamp' method will overwrite another key. For that
/// key we will observe larger access time. When one of the
/// conflicted entries will be deleted another will be deleted
/// as well. This is OK because the code in the
/// 'cloud_storage/cache_service' is ready for that.
class access_time_tracker {
    using timestamp_t = uint32_t;
    struct table_t
      : serde::envelope<table_t, serde::version<0>, serde::compat_version<0>> {
        absl::btree_map<uint32_t, timestamp_t> data;
    };

public:
    /// Add access time to the container.
    void add_timestamp(
      std::string_view key, std::chrono::system_clock::time_point ts);

    /// Remove key from the container.
    void remove_timestamp(std::string_view) noexcept;

    /// Return access time estimate (it can differ if there is a conflict
    /// on file name hash).
    std::optional<std::chrono::system_clock::time_point>
    estimate_timestamp(std::string_view key) const;

    iobuf to_iobuf();
    void from_iobuf(iobuf b);

    /// Returns true if tracker has new data which wasn't serialized
    /// to disk.
    bool is_dirty() const;

    /// Remove every key which isn't present in 't'
    void remove_others(const access_time_tracker& t);

private:
    table_t _table;
    bool _dirty{false};
};

} // namespace cloud_storage
