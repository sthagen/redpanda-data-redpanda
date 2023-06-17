/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "resource_mgmt/storage.h"
#include "seastarx.h"
#include "storage/probe.h"
#include "utils/named_type.h"
#include "utils/notification_list.h"

#include <seastar/core/future.hh>

namespace storage {

/**
 * Node-wide storage state: only maintained on shard 0.
 *
 * The disk usage stats in this class are periodically updated
 * from the code in cluster::node.
 */
class node {
public:
    static constexpr ss::shard_id work_shard = 0;

    using notification_id = named_type<int32_t, struct notification_id_t>;
    using disk_cb_t
      = ss::noncopyable_function<void(uint64_t, uint64_t, disk_space_alert)>;

    enum class disk_type { data, cache };

    node(ss::sstring data_directory, ss::sstring cache_directory);

    ss::future<> start();
    ss::future<> stop();

    void set_disk_metrics(
      disk_type t,
      uint64_t total_bytes,
      uint64_t free_bytes,
      disk_space_alert alert);

    notification_id register_disk_notification(disk_type t, disk_cb_t cb);
    void unregister_disk_notification(disk_type t, notification_id id);

    struct stat_info {
        ss::sstring path;
        struct statvfs stat;
    };

    ss::future<stat_info> get_statvfs(disk_type);

    /*
     * total_bytes: size of disk
     *
     * free_bytes: free space on disk. if not specified then actual free space
     * is used so that statvfs reflects dynamic changes on the file system.
     *
     * free_bytes_delta: signed value applied to free_bytes, for example to
     * simulate a separate application taking up / freeing space independent of
     * redpanda.
     */
    struct statvfs_overrides {
        std::optional<size_t> total_bytes;
        std::optional<size_t> free_bytes;
        std::optional<ssize_t> free_bytes_delta;

        bool has_overrides() const {
            return total_bytes.has_value() || free_bytes.has_value()
                   || free_bytes_delta.has_value();
        }
    };

    void set_statvfs_overrides(disk_type, statvfs_overrides);

private:
    storage::node_probe _probe;

    notification_list<disk_cb_t, notification_id> _data_watchers;
    notification_list<disk_cb_t, notification_id> _cache_watchers;

    statvfs_overrides _data_overrides;
    statvfs_overrides _cache_overrides;

    ss::sstring _data_directory;
    ss::sstring _cache_directory;
};

} // namespace storage
