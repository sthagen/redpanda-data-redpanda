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
#include "ssx/work_queue.h"
#include "utils/named_type.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <expected>

namespace cluster {
class topic_table;
class topics_frontend;
struct nt_revision;
} // namespace cluster

namespace cloud_topics::l1 {

class metastore;

// Removes topic state from the metastore based on cloud topic tombstones that
// are in the topic table, removing the tombstone once successful.
class topic_purger {
public:
    using error = named_type<ss::sstring, struct purger_error_tag>;
    using remove_tombstone_ret_t = std::expected<std::nullopt_t, error>;
    using remove_tombstone_fn_t = ss::noncopyable_function<
      ss::future<remove_tombstone_ret_t>(const cluster::nt_revision&)>;
    topic_purger(
      metastore* metastore,
      cluster::topic_table* topics,
      remove_tombstone_fn_t remove_fn);

    ss::future<std::expected<void, error>>
    purge_tombstoned_topics(ss::abort_source*);

private:
    // Callers are expected to ensure this object outlives the stm and cluster
    // state.
    metastore* metastore_;
    cluster::topic_table* topics_;
    remove_tombstone_fn_t remove_tombstone_;
};

// Manages a loop to purge topics that runs only on leadership of a partition
// (expected that the partition is partition 0 of the metastore topic).
class topic_purge_loop;
class topic_purger_manager {
public:
    using needs_loop = ss::bool_class<struct needs_loop_tag>;
    topic_purger_manager(
      metastore* metastore,
      ss::sharded<cluster::topic_table>* topics,
      ss::sharded<cluster::topics_frontend>* topics_fe);
    ~topic_purger_manager();

    // Enqueues a reset of the loop such that eventually a topic_purge_loop
    // will be running if needs_loop is true, or not running if false.
    void enqueue_loop_reset(needs_loop needs);

    ss::future<> stop();

private:
    ss::future<> reset_purge_loop(needs_loop needs);

    metastore* metastore_;
    ss::sharded<cluster::topic_table>* topics_;
    ss::sharded<cluster::topics_frontend>* topics_fe_;

    ssx::work_queue queue_;
    std::unique_ptr<topic_purge_loop> topic_purge_loop_;
};

} // namespace cloud_topics::l1
