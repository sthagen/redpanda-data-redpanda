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

#include "cluster_link/service.h"

#include "cluster/cluster_link/frontend.h"
#include "cluster/partition_manager.h"
#include "cluster_link/link.h"
#include "cluster_link/logger.h"
#include "cluster_link/manager.h"
#include "cluster_link/model/types.h"
#include "model/namespace.h"
#include "raft/group_manager.h"

namespace cluster_link {

using ::cluster::cluster_link::frontend;

class link_registry_adapter : public link_registry {
public:
    explicit link_registry_adapter(frontend* plf)
      : _plf(plf) {}

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_id(model::id_t id) const override {
        return _plf->find_link_by_id(id);
    }

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t& name) const override {
        return _plf->find_link_by_name(name);
    }

    chunked_vector<model::id_t> get_all_link_ids() const override {
        return _plf->get_all_link_ids();
    }

private:
    frontend* _plf;
};

class default_link_factory : public link_factory {
public:
    std::unique_ptr<link> create_link(model::metadata config) override {
        return std::make_unique<link>(std::move(config));
    }
};

service::service(
  ::model::node_id self,
  ss::sharded<frontend>* plf,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<raft::group_manager>* group_manager)
  : _self(self)
  , _plf(plf)
  , _partition_manager(partition_manager)
  , _group_manager(group_manager) {}

service::~service() = default;

ss::future<> service::start() {
    vlog(cllog.info, "Starting panda link service");
    _manager = std::make_unique<manager>(
      _self,
      std::make_unique<link_registry_adapter>(&_plf->local()),
      std::make_unique<default_link_factory>());

    // Register notifications before the manager starts.  The manager will have
    // a constructed the underlying workqueue to start in a paused state and
    // will pick up the notifications once it has started
    register_notifications();
    co_await _manager->start();
}

ss::future<> service::stop() {
    vlog(cllog.info, "Stopping panda link service");
    unregister_notifications();
    co_await _manager->stop();
}

void service::register_notifications() {
    auto pl_notif_id = _plf->local().register_for_updates(
      [this](model::id_t id) { _manager->on_link_change(id); });
    _notification_cleanups.emplace_back([this, pl_notif_id] {
        _plf->local().unregister_for_updates(pl_notif_id);
    });

    auto leadership_notif_id
      = _group_manager->local().register_leadership_notification(
        [this](
          raft::group_id group_id,
          ::model::term_id term,
          std::optional<::model::node_id> leader) {
            on_leadership_change(group_id, term, leader);
        });
    _notification_cleanups.emplace_back([this, leadership_notif_id] {
        _group_manager->local().unregister_leadership_notification(
          leadership_notif_id);
    });

    auto umanage_notif_id
      = _partition_manager->local().register_unmanage_notification(
        ::model::kafka_namespace, [this](::model::topic_partition_view tp) {
            on_unmanage_notification(tp);
        });
    _notification_cleanups.emplace_back([this, umanage_notif_id] {
        _partition_manager->local().unregister_unmanage_notification(
          umanage_notif_id);
    });
    auto manage_notif_id
      = _partition_manager->local().register_manage_notification(
        ::model::kafka_namespace,
        [this](const ss::lw_shared_ptr<cluster::partition>& p) {
            on_manage_notification(p);
        });
    _notification_cleanups.emplace_back([this, manage_notif_id] {
        _partition_manager->local().unregister_manage_notification(
          manage_notif_id);
    });
}

void service::unregister_notifications() { _notification_cleanups.clear(); }

void service::on_leadership_change(
  raft::group_id group_id,
  ::model::term_id term,
  std::optional<::model::node_id> leader) {
    vlog(
      cllog.trace,
      "on_leadership_change: group_id={}, term={}, leader={}",
      group_id,
      term,
      leader);
    auto partition = _partition_manager->local().partition_for(group_id);
    if (!partition) {
        vlog(
          cllog.debug,
          "got leadership notification for unknown partition: {}",
          group_id);
        return;
    }
    bool node_is_leader = leader.has_value() && *leader == _self;
    if (!node_is_leader) {
        _manager->on_leadership_change(partition->ntp(), ntp_leader::no);
        return;
    }
    auto is_leader = partition->is_leader() ? ntp_leader::yes : ntp_leader::no;
    _manager->on_leadership_change(partition->ntp(), is_leader);
}

void service::on_unmanage_notification(::model::topic_partition_view tp) {
    vlog(
      cllog.trace, "on_unmanage_notification: {}/{}", tp.topic, tp.partition);
    _manager->on_leadership_change(
      ::model::ntp{::model::kafka_namespace, tp.topic, tp.partition},
      ntp_leader::no);
}

void service::on_manage_notification(
  const ss::lw_shared_ptr<cluster::partition>& p) {
    vlog(cllog.trace, "on_manage_notification: {}", p->ntp());
    _manager->on_leadership_change(
      p->ntp(), p->is_elected_leader() ? ntp_leader::yes : ntp_leader::no);
}
} // namespace cluster_link
