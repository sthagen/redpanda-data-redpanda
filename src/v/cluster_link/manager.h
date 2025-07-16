/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/container/flat_hash_map.h"
#include "cluster_link/link.h"
#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "cluster_link/types.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "ssx/work_queue.h"
#include "utils/mutex.h"

namespace cluster_link {

/**
 * @brief Abstract class that provides accessors to cluster link table
 *
 */
class link_registry {
public:
    link_registry() = default;
    link_registry(const link_registry&) = delete;
    link_registry(link_registry&&) = delete;
    link_registry& operator=(const link_registry&) = delete;
    link_registry& operator=(link_registry&&) = delete;
    virtual ~link_registry() = default;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
      find_link_by_id(model::id_t) const = 0;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t&) const = 0;

    virtual chunked_vector<model::id_t> get_all_link_ids() const = 0;
};

/**
 * @brief Factory abstract class to create new links
 *
 */
class link_factory {
public:
    link_factory() = default;
    link_factory(const link_factory&) = delete;
    link_factory(link_factory&&) = delete;
    link_factory& operator=(const link_factory&) = delete;
    link_factory& operator=(link_factory&&) = delete;
    virtual ~link_factory() = default;

    virtual std::unique_ptr<link> create_link(
      ::model::node_id self,
      model::metadata config,
      kafka::data::rpc::partition_leader_cache* partition_leader_cache,
      kafka::data::rpc::partition_manager* partition_manager)
      = 0;
};

/**
 * @brief Class used to manage cluster links
 *
 * This class will be notified of changes in Redpanda to create, modify, or
 * remove cluster links and to handle leadership changes of partitions.
 */
class manager {
public:
    manager(
      ::model::node_id self,
      std::unique_ptr<kafka::data::rpc::partition_leader_cache>
        partition_leader_cache,
      std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager,
      std::unique_ptr<link_registry> registry,
      std::unique_ptr<link_factory> link_factory,
      ss::lowres_clock::duration task_reconciler_interval);
    manager(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(const manager&) = delete;
    manager& operator=(manager&&) = delete;
    virtual ~manager() = default;

    ss::future<> start();
    ss::future<> stop();

    /// Used to notify that a cluster link has been updated
    void on_link_change(model::id_t id);
    /// Used to notify manager in a change of NTP leadership
    void on_leadership_change(::model::ntp ntp, ntp_leader is_ntp_leader);
    /// Handles creation and start of a link
    ss::future<> handle_on_link_change(model::id_t id);
    /// Handles leadership changes for a given NTP
    ss::future<> handle_on_leadership_change(::model::ntp, ntp_leader);

    /// Registers a task factory that will be used to create tasks when links
    /// are created
    template<typename T, typename... Args>
    void register_task_factory(Args&&... args) {
        _task_factories.emplace_back(
          std::make_unique<T>(std::forward<Args>(args)...));
    }

    model::cluster_link_task_status_report get_task_status_report() const;

private:
    /// Called periodically to reconcile registered tasks on created links
    ss::future<> link_task_reconciler();

private:
    ::model::node_id _self;
    std::unique_ptr<kafka::data::rpc::partition_leader_cache>
      _partition_leader_cache;
    std::unique_ptr<kafka::data::rpc::partition_manager> _partition_manager;
    std::unique_ptr<link_registry> _registry;
    std::unique_ptr<link_factory> _link_factory;
    ssx::work_queue _queue;

    chunked_vector<std::unique_ptr<task_factory>> _task_factories;
    absl::flat_hash_map<id_t, std::unique_ptr<link>> _links;

    ss::lowres_clock::duration _task_reconciler_interval;
    mutex _link_task_reconciler_mutex{
      "cluster_link::manager::link_task_reconciler"};
    ss::timer<ss::lowres_clock> _link_task_reconciler_timer;
    ss::abort_source _as;
    ss::gate _g;
};
} // namespace cluster_link
