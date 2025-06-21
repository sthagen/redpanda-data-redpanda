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

#include "base/seastarx.h"
#include "cluster/cluster_link/fwd.h"
#include "cluster/fwd.h"
#include "cluster_link/fwd.h"
#include "model/fundamental.h"
#include "raft/fundamental.h"
#include "raft/fwd.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace cluster_link {
/**
 * @brief API access for panda link service
 */
class service {
public:
    service(
      ::model::node_id self,
      ss::sharded<::cluster::cluster_link::frontend>* plf,
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<raft::group_manager>* group_manager);

    service(const service&) = delete;
    service(service&&) = delete;
    service& operator=(const service&) = delete;
    service& operator=(service&&) = delete;
    virtual ~service();

    ss::future<> start();
    ss::future<> stop();

private:
    void register_notifications();
    void unregister_notifications();

    void on_leadership_change(
      raft::group_id group_id,
      model::term_id term,
      std::optional<model::node_id> leader);

    void on_unmanage_notification(model::topic_partition_view tp);
    void on_manage_notification(const ss::lw_shared_ptr<cluster::partition>& p);

private:
    ss::gate _gate;
    model::node_id _self;
    ss::sharded<::cluster::cluster_link::frontend>* _plf;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<raft::group_manager>* _group_manager;
    std::unique_ptr<manager> _manager;
    std::vector<ss::deferred_action<ss::noncopyable_function<void()>>>
      _notification_cleanups;
};
} // namespace cluster_link
