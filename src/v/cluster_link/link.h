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

#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "cluster_link/types.h"
#include "kafka/client/cluster.h"
#include "kafka/data/rpc/deps.h"
#include "utils/mutex.h"
#include "utils/notification_list.h"

namespace cluster_link {
/**
 * @brief The link class represents a link between two clusters
 */
class link {
public:
    explicit link(
      ::model::node_id self,
      ss::lowres_clock::duration task_reconciler_interval,
      model::metadata config,
      kafka::data::rpc::partition_leader_cache* partition_leader_cache,
      kafka::data::rpc::partition_manager* partition_manager,
      kafka::client::cluster cluster_connection);
    link(const link&) = delete;
    link(link&&) = delete;
    link& operator=(const link&) = delete;
    link& operator=(link&&) = delete;
    virtual ~link() = default;

    virtual ss::future<> start();
    virtual ss::future<> stop();

    ss::future<result<void>> register_task(task_factory*);

    void update_config(model::metadata);

    ss::future<>
    handle_on_leadership_change(::model::ntp ntp, ntp_leader is_ntp_leader);

    const model::metadata& config() const;

    bool task_is_registered(std::string_view) const noexcept;

    using task_state_notification_id
      = named_type<size_t, struct task_state_notification_id_tag>;
    /// Callback for when a task changes state.  Callback reports the link name,
    /// task name, and the state change information
    using task_state_change_cb = ss::noncopyable_function<void(
      model::name_t, std::string_view, task::state_change)>;

    task_state_notification_id
    register_for_task_state_changes(task_state_change_cb cb);

    void
    unregister_for_task_state_changes(task_state_notification_id id) noexcept;

    model::link_task_status_report get_task_status_report() const;

private:
    bool should_start_task(task* t) const;
    bool should_stop_task(task* t) const;
    ss::future<> handle_controller_leadership_change(ntp_leader is_ntp_leader);
    ss::future<>
    do_handle_controller_leadership_change(ntp_leader is_ntp_leader);
    ss::future<> run_task_reconciler();
    ss::future<result<void>> do_register_task(std::unique_ptr<task>);

private:
    ::model::node_id _self;
    chunked_hash_map<ss::sstring, std::unique_ptr<task>> _tasks;
    model::metadata _config;
    kafka::data::rpc::partition_leader_cache* _partition_leader_cache;
    kafka::data::rpc::partition_manager* _partition_manager;
    kafka::client::cluster _cluster_connection;

    notification_list<task_state_change_cb, task_state_notification_id>
      _task_state_change_notifications;
    ss::lowres_clock::duration _task_reconciler_interval;
    mutex _task_reconciler_mutex{"cluster_link::link::task_reconciler"};
    ss::timer<ss::lowres_clock> _task_reconciler;
    ss::gate _gate;
    ss::abort_source _as;
    bool _is_running{false};
};
} // namespace cluster_link
