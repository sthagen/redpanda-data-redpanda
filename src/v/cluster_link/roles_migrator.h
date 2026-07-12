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

#pragma once

#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "cluster_link/utils.h"
#include "kafka/protocol/describe_redpanda_roles.h"
#include "kafka/protocol/types.h"

namespace cluster_link {

/// Periodically full-mirrors RBAC roles from the source cluster onto the
/// shadow cluster, within the configured role-name filter scope.
class roles_migrator : public controller_locked_task {
public:
    static constexpr auto task_name = "Roles Migrator Task";
    static constexpr auto required_permissions = cluster_describe_permission;

    roles_migrator(link* link, const model::metadata& config);
    roles_migrator(const roles_migrator&) = delete;
    roles_migrator(roles_migrator&&) = delete;
    roles_migrator& operator=(const roles_migrator&) = delete;
    roles_migrator& operator=(roles_migrator&&) = delete;
    ~roles_migrator() override = default;

    void update_config(const model::metadata& config) override;

    model::enabled_t is_enabled() const final;

protected:
    ss::future<state_transition> run_impl(ss::abort_source&) override;

private:
    model::role_sync_config _config;
};

class roles_migrator_factory : public task_factory {
public:
    std::string_view created_task_name() const noexcept final;
    std::unique_ptr<task> create_task(link* link) final;
};

} // namespace cluster_link
