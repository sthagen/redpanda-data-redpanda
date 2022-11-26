// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/members_table.h"

#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "vlog.h"

#include <algorithm>
#include <vector>

namespace cluster {

const members_table::cache_t& members_table::nodes() const { return _nodes; }

std::vector<node_metadata> members_table::node_list() const {
    std::vector<node_metadata> nodes;
    nodes.reserve(_nodes.size());
    for (const auto& [_, meta] : _nodes) {
        nodes.push_back(meta);
    }
    return nodes;
}
size_t members_table::node_count() const { return _nodes.size(); }

std::vector<model::node_id> members_table::node_ids() const {
    std::vector<model::node_id> ids;
    ids.reserve(_nodes.size());
    for (const auto& [id, _] : _nodes) {
        ids.push_back(id);
    }
    return ids;
}

std::optional<std::reference_wrapper<const node_metadata>>
members_table::get_node_metadata_ref(model::node_id id) const {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        return std::nullopt;
    }
    return std::make_optional(std::cref(it->second));
}

std::optional<node_metadata>
members_table::get_node_metadata(model::node_id id) const {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        return std::nullopt;
    }
    return it->second;
}

void members_table::update_brokers(
  model::offset version, const std::vector<model::broker>& new_brokers) {
    _version = model::revision_id(version());

    for (auto& br : new_brokers) {
        auto it = _nodes.find(br.id());
        if (it != _nodes.end()) {
            // update configuration
            it->second.broker = br;

        } else {
            _nodes.emplace(
              br.id(),
              node_metadata{
                .broker = br,
                .state = broker_state{},
              });
        }

        _waiters.notify(br.id());
    }
    for (auto it = _nodes.begin(); it != _nodes.end();) {
        auto new_it = std::find_if(
          new_brokers.begin(),
          new_brokers.end(),
          [id = it->first](const model::broker& br) { return br.id() == id; });
        if (new_it == new_brokers.end()) {
            _removed_nodes.emplace(it->first, it->second);
            _nodes.erase(it++);
            continue;
        }
        ++it;
    }

    notify_members_updated();
}
std::error_code
members_table::apply(model::offset version, decommission_node_cmd cmd) {
    _version = model::revision_id(version());

    if (auto it = _nodes.find(cmd.key); it != _nodes.end()) {
        auto& [id, metadata] = *it;
        if (
          metadata.state.get_membership_state()
          != model::membership_state::active) {
            return errc::invalid_node_operation;
        }
        vlog(
          clusterlog.info,
          "changing node {} membership state to: {}",
          id,
          model::membership_state::draining);
        metadata.state.set_membership_state(model::membership_state::draining);
        return errc::success;
    }
    return errc::node_does_not_exists;
}

std::error_code
members_table::apply(model::offset version, recommission_node_cmd cmd) {
    _version = model::revision_id(version());

    if (auto it = _nodes.find(cmd.key); it != _nodes.end()) {
        auto& [id, metadata] = *it;
        if (
          metadata.state.get_membership_state()
          != model::membership_state::draining) {
            return errc::invalid_node_operation;
        }
        vlog(
          clusterlog.info,
          "changing node {} membership state to: {}",
          id,
          model::membership_state::active);
        metadata.state.set_membership_state(model::membership_state::active);
        return errc::success;
    }
    return errc::node_does_not_exists;
}

std::error_code
members_table::apply(model::offset version, maintenance_mode_cmd cmd) {
    _version = model::revision_id(version());

    const auto target = _nodes.find(cmd.key);
    if (target == _nodes.end()) {
        return errc::node_does_not_exists;
    }
    auto& [id, metadata] = *target;

    // no rules to enforce when disabling maintenance mode
    const auto enable = cmd.value;
    if (!enable) {
        if (
          metadata.state.get_maintenance_state()
          == model::maintenance_state::inactive) {
            vlog(
              clusterlog.trace, "node {} already not in maintenance state", id);
            return errc::success;
        }

        vlog(clusterlog.info, "marking node {} not in maintenance state", id);
        metadata.state.set_maintenance_state(
          model::maintenance_state::inactive);
        notify_maintenance_state_change(id, model::maintenance_state::inactive);

        return errc::success;
    }

    if (_nodes.size() < 2) {
        // Maintenance mode is refused on size 1 clusters in the admin API, but
        // we might be upgrading from a version that didn't have the validation.
        vlog(
          clusterlog.info,
          "Dropping maintenance mode enable operation on single node cluster");

        // Return success to enable progress: this is a clean no-op.
        return errc::success;
    }

    if (
      metadata.state.get_maintenance_state()
      == model::maintenance_state::active) {
        vlog(clusterlog.trace, "node {} already in maintenance state", id);
        return errc::success;
    }

    /*
     * enforce one-node-at-a-time in maintenance mode rule
     */
    const auto other = std::find_if(
      _nodes.cbegin(), _nodes.cend(), [](const auto& b) {
          return b.second.state.get_maintenance_state()
                 == model::maintenance_state::active;
      });

    if (other != _nodes.cend()) {
        vlog(
          clusterlog.info,
          "cannot place node {} into maintenance mode. node {} already in "
          "maintenance mode",
          id,
          other->first);
        return errc::invalid_node_operation;
    }

    vlog(clusterlog.info, "marking node {} in maintenance state", id);

    metadata.state.set_maintenance_state(model::maintenance_state::active);

    notify_maintenance_state_change(id, model::maintenance_state::active);

    return errc::success;
}

bool members_table::contains(model::node_id id) const {
    return _nodes.contains(id);
}

notification_id_type
members_table::register_maintenance_state_change_notification(
  maintenance_state_cb_t cb) {
    auto id = _maintenance_state_change_notification_id++;
    _maintenance_state_change_notifications.emplace_back(id, std::move(cb));
    return id;
}

void members_table::unregister_maintenance_state_change_notification(
  notification_id_type id) {
    auto it = std::find_if(
      _maintenance_state_change_notifications.begin(),
      _maintenance_state_change_notifications.end(),
      [id](const auto& n) { return n.first == id; });
    if (it != _maintenance_state_change_notifications.end()) {
        _maintenance_state_change_notifications.erase(it);
    }
}

void members_table::notify_maintenance_state_change(
  model::node_id node_id, model::maintenance_state ms) {
    for (const auto& [id, cb] : _maintenance_state_change_notifications) {
        cb(node_id, ms);
    }
}

notification_id_type
members_table::register_members_updated_notification(members_updated_cb_t cb) {
    auto id = _members_updated_notification_id++;
    _members_updated_notifications.emplace_back(id, std::move(cb));

    return id;
}

void members_table::unregister_members_updated_notification(
  notification_id_type id) {
    auto it = std::find_if(
      _members_updated_notifications.begin(),
      _members_updated_notifications.end(),
      [id](const auto& n) { return n.first == id; });
    if (it != _members_updated_notifications.end()) {
        _members_updated_notifications.erase(it);
    }
}

void members_table::notify_members_updated() {
    for (const auto& [id, cb] : _members_updated_notifications) {
        cb(node_ids());
    }
}

} // namespace cluster
