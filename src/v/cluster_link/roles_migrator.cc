/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#include "cluster_link/roles_migrator.h"

#include "cluster_link/link.h"
#include "cluster_link/model/filter_utils.h"
#include "cluster_link/role_reconcile.h"
#include "cluster_link/utils.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/details/roles.h"

#include <seastar/coroutine/as_future.hh>

using namespace std::chrono_literals;

namespace cluster_link {

namespace {
constexpr auto role_apply_timeout = 5s;
constexpr auto role_apply_concurrency = 5;
} // namespace

roles_migrator::roles_migrator(link* link, const model::metadata& config)
  : controller_locked_task(
      link,
      config.configuration.role_sync_cfg.get_task_interval(),
      roles_migrator::task_name)
  , _config(config.configuration.role_sync_cfg.copy()) {}

void roles_migrator::update_config(const model::metadata& config) {
    _config = config.configuration.role_sync_cfg.copy();
    set_run_interval(config.configuration.role_sync_cfg.get_task_interval());
}

model::enabled_t roles_migrator::is_enabled() const {
    return _config.is_enabled;
}

ss::future<task::state_transition>
roles_migrator::run_impl(ss::abort_source& as) {
    vlog(logger().trace, "Running roles migrator task");

    if (_config.role_name_filters.empty()) {
        vlog(logger().debug, "No role filters configured, skipping task");
        co_return state_transition{
          .desired_state = model::task_state::active,
          .reason = "No role filters configured, skipping task"};
    }

    if (!get_link()->get_security_service().rbac_active()) {
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = "RBAC disabled on the shadow cluster (cannot sync roles)"};
    }

    auto& cluster = get_link()->get_cluster_connection();

    try {
        co_await cluster.request_metadata_update(std::nullopt);
    } catch (const std::exception& e) {
        auto msg = ssx::sformat("Failed to update metadata: {}", e.what());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    as.check();

    if (!has_required_permissions(
          cluster.get_cluster_authorized_operations(),
          roles_migrator::required_permissions)) {
        auto msg = ssx::sformat(
          "Shadow link client has insufficient permissions on the source "
          "cluster to replicate roles. Requires {:08x}, has {:08x}",
          roles_migrator::required_permissions,
          cluster.get_cluster_authorized_operations());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    auto version_res
      = co_await negotiate_api_version<kafka::describe_redpanda_roles_api>(
        cluster, as);
    if (!version_res.has_value()) {
        vlog(logger().warn, "{}", version_res.error());
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(version_res).error()};
    }
    auto version = version_res.value();

    as.check();

    // Fetch ALL roles in a single request (null filter). Single request => no
    // partial fetch => deletes are only ever computed from a complete success.
    kafka::describe_redpanda_roles_request req;
    auto resp_f = co_await ss::coroutine::as_future(
      cluster.dispatch_to_any(std::move(req), version));
    if (resp_f.failed()) {
        auto ex = resp_f.get_exception();
        auto level = ssx::is_shutdown_exception(ex) ? ss::log_level::trace
                                                    : ss::log_level::warn;
        auto msg = ssx::sformat("Failed to fetch roles: {}", ex);
        vlogl(logger(), level, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }
    auto resp = std::move(resp_f).get();
    if (resp.data.error_code != kafka::error_code::none) {
        auto msg = ssx::sformat(
          "DescribeRedpandaRoles returned error: {}{}",
          resp.data.error_code,
          resp.data.error_message.has_value()
            ? " (" + resp.data.error_message.value() + ")"
            : "");
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    const auto predicate = [this](const security::role_name& name) {
        return model::select_role(name(), _config.role_name_filters);
    };

    chunked_vector<security::role_with_members> source_selected;
    try {
        for (const auto& role : resp.data.roles) {
            auto rwm = kafka::details::to_role_with_members(role);
            if (predicate(rwm.name)) {
                source_selected.push_back(std::move(rwm));
            }
        }
    } catch (const std::exception& e) {
        auto msg = ssx::sformat(
          "Error parsing roles from source cluster: {}", e.what());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::faulted,
          .reason = std::move(msg)};
    }

    auto shadow_selected = get_link()->get_security_service().read_shadow_roles(
      predicate);

    auto changes = reconcile_roles(
      std::move(source_selected), std::move(shadow_selected));

    as.check();

    size_t failures = 0;
    size_t created = 0;
    size_t updated = 0;
    size_t deleted = 0;
    bool rbac_disabled = false;
    auto& security = get_link()->get_security_service();

    co_await ss::max_concurrent_for_each(
      changes.to_create,
      role_apply_concurrency,
      [&](this auto, security::role_with_members& r) -> ss::future<> {
          if (rbac_disabled) {
              co_return;
          }
          auto ec = co_await security.create_role(
            r.name, std::move(r.role), role_apply_timeout);
          if (ec == cluster::errc::feature_disabled) {
              rbac_disabled = true;
          } else if (ec == cluster::errc::role_exists) {
              // role_exists is a benign race: the role appeared since the
              // reconcile snapshot, and the next cycle reconciles it.
              vlog(
                logger().trace,
                "Skipping create of already-existing role {}",
                r.name);
          } else if (ec) {
              vlog(logger().warn, "Failed to create role {}: {}", r.name, ec);
              ++failures;
          } else {
              ++created;
          }
      });
    as.check();

    co_await ss::max_concurrent_for_each(
      changes.to_update,
      role_apply_concurrency,
      [&](this auto, security::role_with_members& r) -> ss::future<> {
          if (rbac_disabled) {
              co_return;
          }
          auto ec = co_await security.update_role(
            r.name, std::move(r.role), role_apply_timeout);
          if (ec == cluster::errc::feature_disabled) {
              rbac_disabled = true;
          } else if (ec == cluster::errc::role_does_not_exist) {
              // role_does_not_exist is a benign race: the role was deleted
              // since the reconcile snapshot, and the next cycle reconciles it.
              vlog(
                logger().trace, "Skipping update of vanished role {}", r.name);
          } else if (ec) {
              vlog(logger().warn, "Failed to update role {}: {}", r.name, ec);
              ++failures;
          } else {
              ++updated;
          }
      });
    as.check();

    co_await ss::max_concurrent_for_each(
      changes.to_delete,
      role_apply_concurrency,
      [&](this auto, security::role_name& name) -> ss::future<> {
          if (rbac_disabled) {
              co_return;
          }
          auto ec = co_await security.delete_role(name, role_apply_timeout);
          if (ec == cluster::errc::feature_disabled) {
              rbac_disabled = true;
          } else if (ec && ec != cluster::errc::role_does_not_exist) {
              // role_does_not_exist is a benign race: the role was already
              // gone since the reconcile snapshot, which is the desired state.
              ++failures;
              vlog(logger().warn, "Failed to delete role {}: {}", name, ec);
          } else {
              ++deleted;
          }
      });

    auto summary = ssx::sformat(
      "{} created, {} updated, {} deleted, {} failures",
      created,
      updated,
      deleted,
      failures);

    if (rbac_disabled) {
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = ssx::sformat(
            "RBAC disabled on the shadow cluster ({} before RBAC was "
            "disabled)",
            summary)};
    }

    co_return state_transition{
      .desired_state = failures > 0 ? model::task_state::faulted
                                    : model::task_state::active,
      .reason = ssx::sformat("Synced roles: {}", summary)};
}

std::string_view roles_migrator_factory::created_task_name() const noexcept {
    return roles_migrator::task_name;
}

std::unique_ptr<task> roles_migrator_factory::create_task(link* link) {
    return std::make_unique<roles_migrator>(link, *(link->get_config()));
}

} // namespace cluster_link
