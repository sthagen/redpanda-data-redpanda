/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/roles_migrator.h"
#include "cluster_link/tests/deps.h"
#include "kafka/protocol/describe_redpanda_roles.h"
#include "kafka/server/handlers/details/roles.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>

using namespace std::chrono_literals;

namespace cluster_link::tests {
namespace {
static const auto link_name = model::name_t("test_link");

model::metadata get_default_metadata() {
    model::role_sync_config role_cfg;
    role_cfg.task_interval = 1s;
    role_cfg.role_name_filters.emplace_back(
      model::resource_name_filter_pattern{
        .pattern_type = model::filter_pattern_type::prefix,
        .filter = model::filter_type::include,
        .pattern = "synced-"});

    model::metadata md{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::
        connection_config{.bootstrap_servers = {net::unresolved_address("localhost", 9092)}},
      .state = model::link_state{}};
    md.configuration.role_sync_cfg = std::move(role_cfg);
    return md;
}

security::role_member user(std::string_view n) {
    return security::role_member{
      security::role_member_type::user, ss::sstring{n}};
}

// Builds a role with user-type members, mirroring how the migrator maps a
// wire role to security::role_with_members.
security::role_with_members rwm(
  std::string_view name, std::initializer_list<security::role_member> members) {
    return security::role_with_members{
      .name = security::role_name{ss::sstring{name}},
      .role = security::role{security::role::container_type{members}}};
}

} // namespace

class roles_migrator_test : public seastar_test {
public:
    static constexpr auto task_reconciler_interval = 1s;

    ss::future<> SetUpAsync() override {
        _clmtf = std::make_unique<cluster_link_manager_test_fixture>(self());
        co_await _clmtf->wire_up_and_start(
          std::make_unique<test_link_factory>(task_reconciler_interval));

        _clmtf->get_cluster_mock().set_cluster_authorized_operations(
          kafka::cluster_authorized_operations(0x100));

        // Advertise describe_redpanda_roles_api at version 0 on all brokers
        // so that the migrator's version negotiation succeeds.
        for (auto nid : _clmtf->get_cluster_mock().get_broker_ids()) {
            _clmtf->get_cluster_mock().set_supported_versions(
              nid,
              kafka::describe_redpanda_roles_api::key,
              kafka::client::api_version_range{
                .min = kafka::api_version(0), .max = kafka::api_version(0)});
        }

        _clmtf->get_cluster_mock().register_handler(
          kafka::describe_redpanda_roles_api::key,
          [this](
            ::model::node_id, kafka::client::request_t, kafka::api_version) {
              kafka::describe_redpanda_roles_response resp;
              resp.data.error_code = _fetch_error;
              resp.data.roles = _source_roles
                                | std::views::transform(
                                  kafka::details::to_wire_redpanda_role)
                                | std::ranges::to<decltype(resp.data.roles)>();
              return ss::make_ready_future<kafka::client::response_t>(
                kafka::client::response_t{std::move(resp)});
          });

        co_await _clmtf->get_manager().invoke_on_all([](manager& m) {
            return m.register_task_factory<roles_migrator_factory>();
        });

        fixture().elect_leader(::model::controller_ntp, self(), std::nullopt);
    }

    ss::future<> TearDownAsync() override {
        co_await _clmtf->reset();
        _clmtf.reset();
    }

    ::model::node_id self() { return ::model::node_id(0); }
    cluster_link_manager_test_fixture& fixture() { return *_clmtf; }

    // Waits (up to 5s) for the roles migrator task to report the given state.
    // Returns false on timeout.
    ss::future<bool> wait_for_roles_task_state(model::task_state want) {
        return fixture().wait_for_report_to_match(
          5s, 200ms, [want](const model::cluster_link_task_status_report& r) {
              auto link_it = r.link_reports.find(link_name);
              if (link_it == r.link_reports.end()) {
                  return false;
              }
              auto task_it = link_it->second.task_status_reports.find(
                roles_migrator::task_name);
              if (task_it == link_it->second.task_status_reports.end()) {
                  return false;
              }
              return task_it->second.task_state == want;
          });
    }

    // Sets the roles (and optional top-level error) the mocked source cluster
    // returns from DescribeRedpandaRoles.
    void set_source_roles(
      std::vector<security::role_with_members> roles,
      kafka::error_code ec = kafka::error_code::none) {
        _source_roles = std::move(roles);
        _fetch_error = ec;
    }

private:
    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;
    std::vector<security::role_with_members> _source_roles;
    kafka::error_code _fetch_error{kafka::error_code::none};
};

TEST_F_CORO(roles_migrator_test, fetch_and_apply) {
    set_source_roles(
      {rwm("synced-role", {user("u1")}),
       rwm("excluded-role", {user("u2")})}); // out of scope

    co_await fixture().upsert_link(get_default_metadata());

    // The in-scope role is mirrored.
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        return fixture().security_service().roles().contains(
          security::role_name{"synced-role"});
    });
    // Out-of-scope role must NOT be mirrored.
    EXPECT_FALSE(
      fixture().security_service().roles().contains(
        security::role_name{"excluded-role"}));
}

TEST_F_CORO(roles_migrator_test, membership_update_propagation) {
    set_source_roles({rwm("synced-role", {user("u1")})});

    co_await fixture().upsert_link(get_default_metadata());

    // Initial create with a single member.
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        const auto& roles = fixture().security_service().roles();
        auto it = roles.find(security::role_name{"synced-role"});
        return it != roles.end() && it->second.members().size() == 1;
    });

    // The source role gains a member; the change must propagate as a full
    // member-set replace through update_role.
    set_source_roles({rwm("synced-role", {user("u1"), user("u2")})});

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        const auto& roles = fixture().security_service().roles();
        auto it = roles.find(security::role_name{"synced-role"});
        return it != roles.end()
               && it->second.members().contains(
                 security::role_member{security::role_member_type::user, "u1"})
               && it->second.members().contains(
                 security::role_member{security::role_member_type::user, "u2"});
    });
}

TEST_F_CORO(roles_migrator_test, deletion_propagation) {
    // Destination has an in-scope role the source does not; it must be pruned.
    fixture().security_service().seed_role(
      security::role_name{"synced-stale"}, security::role{});
    set_source_roles({});

    co_await fixture().upsert_link(get_default_metadata());

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().roles().empty(); });
}

TEST_F_CORO(roles_migrator_test, fail_safe_on_fetch_error) {
    // Seed an in-scope dest role, then make the source return a top-level
    // error.
    fixture().security_service().seed_role(
      security::role_name{"synced-keep"}, security::role{});
    set_source_roles({}, kafka::error_code::broker_not_available);

    co_await fixture().upsert_link(get_default_metadata());

    // The task must actually reach link_unavailable (not merely time out)...
    auto reached = co_await wait_for_roles_task_state(
      model::task_state::link_unavailable);
    EXPECT_TRUE(reached);
    // ...and the role survives a failed fetch.
    EXPECT_TRUE(
      fixture().security_service().roles().contains(
        security::role_name{"synced-keep"}));
}

TEST_F_CORO(roles_migrator_test, no_filters_syncs_nothing) {
    // Role sync is enabled but no filters are configured: nothing is synced and
    // the shadow cluster is left untouched. (paused is reserved for user
    // action, so the task simply stays idle.)
    fixture().security_service().seed_role(
      security::role_name{"role-a"}, security::role{});
    set_source_roles({rwm("role-b", {user("u1")})});

    auto md = get_default_metadata();
    md.configuration.role_sync_cfg.role_name_filters.clear();
    co_await fixture().upsert_link(std::move(md));

    // Let the periodic task fire several times; assert it makes no changes.
    co_await ss::sleep(3 * task_reconciler_interval);

    EXPECT_TRUE( // seeded role untouched (not pruned)
      fixture().security_service().roles().contains(
        security::role_name{"role-a"}));
    EXPECT_FALSE( // and nothing was created
      fixture().security_service().roles().contains(
        security::role_name{"role-b"}));
}

TEST_F_CORO(roles_migrator_test, link_unavailable_when_rbac_inactive) {
    // RBAC inactive on the shadow cluster is a preflight gate: the migrator
    // parks in link_unavailable and mirrors nothing, even with an in-scope
    // source role waiting to sync.
    fixture().security_service().set_rbac_active(false);
    set_source_roles({rwm("synced-role", {user("u1")})});

    co_await fixture().upsert_link(get_default_metadata());

    auto reached = co_await wait_for_roles_task_state(
      model::task_state::link_unavailable);
    EXPECT_TRUE(reached);
    EXPECT_TRUE(fixture().security_service().roles().empty());

    // Enabling RBAC clears the gate; the next reconcile cycle mirrors the role.
    fixture().security_service().set_rbac_active(true);
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        return fixture().security_service().roles().contains(
          security::role_name{"synced-role"});
    });
}

TEST_F_CORO(roles_migrator_test, link_unavailable_when_missing_permission) {
    // Without cluster DESCRIBE permission on the source the migrator cannot
    // safely enumerate roles, so it parks in link_unavailable before fetching
    // and mirrors nothing, even with an in-scope source role waiting to sync.
    fixture().get_cluster_mock().set_cluster_authorized_operations(
      kafka::cluster_authorized_operations(0));
    set_source_roles({rwm("synced-role", {user("u1")})});

    co_await fixture().upsert_link(get_default_metadata());

    auto reached = co_await wait_for_roles_task_state(
      model::task_state::link_unavailable);
    EXPECT_TRUE(reached);
    EXPECT_TRUE(fixture().security_service().roles().empty());
}

TEST_F_CORO(roles_migrator_test, filter_change_rescopes_sync) {
    // A filter change at runtime re-scopes what the sync manages on the next
    // reconcile cycle. A role that enters scope is mirrored; a role that leaves
    // scope is orphaned -- it survives on the shadow cluster even once the
    // source drops it, whereas a still-in-scope role is pruned. Filtering is
    // symmetric across source and shadow, so the deselected role is absent from
    // both reconcile snapshots and is never considered for deletion. (Were the
    // filter applied to the source only, the orphan would appear in the shadow
    // snapshot but not the source one and get pruned.)
    set_source_roles(
      {rwm("synced-role", {user("u1")}),
       rwm(
         "rescoped-role",
         {user("u2")})}); // out of scope under the initial filter

    co_await fixture().upsert_link(get_default_metadata());

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        return fixture().security_service().roles().contains(
          security::role_name{"synced-role"});
    });

    // Shift the filter from the "synced-" prefix to the "rescoped-" prefix:
    // synced-role leaves scope (becoming an orphan) and rescoped-role enters.
    auto md = co_await fixture().find_link_by_name(link_name)->copy();
    md.configuration.role_sync_cfg.role_name_filters.clear();
    md.configuration.role_sync_cfg.role_name_filters.emplace_back(
      model::resource_name_filter_pattern{
        .pattern_type = model::filter_pattern_type::prefix,
        .filter = model::filter_type::include,
        .pattern = "rescoped-"});
    auto link_id = fixture().find_link_id_by_name(link_name);
    ASSERT_TRUE_CORO(link_id.has_value());
    co_await fixture().update_link(*link_id, std::move(md));

    // rescoped-role can only be mirrored under the new filter, so its
    // appearance is a positive signal that the filter change has taken effect.
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        return fixture().security_service().roles().contains(
          security::role_name{"rescoped-role"});
    });

    // With the new filter live, clear the source. The in-scope role
    // (rescoped-role) is pruned; the out-of-scope orphan (synced-role) is left
    // untouched.
    set_source_roles({});

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        return !fixture().security_service().roles().contains(
          security::role_name{"rescoped-role"});
    });
    EXPECT_TRUE(
      fixture().security_service().roles().contains(
        security::role_name{"synced-role"}));
}

TEST_F_CORO(roles_migrator_test, link_unavailable_when_api_unsupported) {
    // The source doesn't advertise DescribeRedpandaRoles at all (e.g. a vanilla
    // Kafka cluster, or a Redpanda predating the custom API), so version
    // negotiation finds no supported version -> link_unavailable.
    for (auto nid : fixture().get_cluster_mock().get_broker_ids()) {
        fixture().get_cluster_mock().remove_supported_version(
          nid, kafka::describe_redpanda_roles_api::key);
    }
    set_source_roles({rwm("synced-role", {user("u1")})});

    co_await fixture().upsert_link(get_default_metadata());

    auto reached = co_await wait_for_roles_task_state(
      model::task_state::link_unavailable);
    EXPECT_TRUE(reached);
    EXPECT_TRUE(fixture().security_service().roles().empty());
}

} // namespace cluster_link::tests
