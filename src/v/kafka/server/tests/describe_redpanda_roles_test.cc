// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/security_frontend.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/describe_redpanda_roles.h"
#include "redpanda/tests/fixture.h"
#include "security/role.h"
#include "security/scram_algorithm.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "test_utils/boost_fixture.h"

#include <map>
#include <set>
#include <vector>

using namespace std::chrono_literals;

namespace {

// Wire member_type (int8) + name, comparable and order-independent.
using member_set = std::set<std::pair<int8_t, ss::sstring>>;

std::map<ss::sstring, member_set>
roles_to_map(const kafka::describe_redpanda_roles_response_data& d) {
    std::map<ss::sstring, member_set> out;
    for (const auto& r : d.roles) {
        member_set ms;
        for (const auto& m : r.members) {
            ms.emplace(m.member_type, m.name);
        }
        out.emplace(r.name, std::move(ms));
    }
    return out;
}

} // namespace

class describe_redpanda_roles_fixture : public redpanda_thread_fixture {
protected:
    void create_role(
      const ss::sstring& name, std::vector<security::role_member> members) {
        security::role::container_type m(members.begin(), members.end());
        auto ec = app.controller->get_security_frontend()
                    .local()
                    .create_role(
                      security::role_name{name},
                      security::role{std::move(m)},
                      model::timeout_clock::now() + 5s)
                    .get();
        BOOST_REQUIRE(!ec);
    }

    void create_user(
      const ss::sstring& username, security::scram_credential credentials) {
        app.controller->get_security_frontend()
          .local()
          .create_user(
            security::credential_user{username},
            std::move(credentials),
            model::timeout_clock::now() + 5s)
          .get();
    }
};

FIXTURE_TEST(describe_all_roles, describe_redpanda_roles_fixture) {
    wait_for_controller_leadership().get();

    create_role(
      "role-a",
      {security::role_member{security::role_member_type::user, "alice"}});
    create_role(
      "role-b",
      {security::role_member{security::role_member_type::group, "admins"}});

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_redpanda_roles_request req;
    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();

    BOOST_REQUIRE_EQUAL(resp.data.error_code, kafka::error_code::none);

    auto got = roles_to_map(resp.data);
    std::map<ss::sstring, member_set> expected{
      {"role-a", {{int8_t(0), "alice"}}},
      {"role-b", {{int8_t(1), "admins"}}},
    };
    BOOST_REQUIRE(got == expected);
}

FIXTURE_TEST(describe_empty_cluster, describe_redpanda_roles_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_redpanda_roles_request req;
    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();

    BOOST_REQUIRE_EQUAL(resp.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE(resp.data.roles.empty());
}

FIXTURE_TEST(describe_roles_unauthorized, describe_redpanda_roles_fixture) {
    wait_for_controller_leadership().get();

    create_role(
      "role-a",
      {security::role_member{security::role_member_type::user, "alice"}});

    // A non-superuser principal without an ACL granting cluster DESCRIBE must
    // be rejected. Create the user, enable SASL, and authenticate as it.
    create_user(
      "test-user",
      security::scram_sha256::make_credentials(
        "password", security::scram_sha256::min_iterations));
    enable_sasl();
    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    authn_kafka_client(client, "test-user", "password");

    kafka::describe_redpanda_roles_request req;
    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();

    BOOST_REQUIRE_EQUAL(
      resp.data.error_code, kafka::error_code::cluster_authorization_failed);
    BOOST_REQUIRE(resp.data.roles.empty());
}

FIXTURE_TEST(describe_filtered_roles, describe_redpanda_roles_fixture) {
    wait_for_controller_leadership().get();

    create_role(
      "role-a",
      {security::role_member{security::role_member_type::user, "alice"}});
    create_role(
      "role-b",
      {security::role_member{security::role_member_type::group, "admins"}});

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_redpanda_roles_request req;
    chunked_vector<kafka::role_name_filter> filters;
    filters.push_back(kafka::role_name_filter{.name = "role-b"});
    req.data.role_name_filters = std::move(filters);

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();

    BOOST_REQUIRE_EQUAL(resp.data.error_code, kafka::error_code::none);

    auto got = roles_to_map(resp.data);
    std::map<ss::sstring, member_set> expected{
      {"role-b", {{int8_t(1), "admins"}}},
    };
    BOOST_REQUIRE(got == expected);
}

FIXTURE_TEST(describe_filter_skips_missing, describe_redpanda_roles_fixture) {
    wait_for_controller_leadership().get();

    create_role(
      "role-a",
      {security::role_member{security::role_member_type::user, "alice"}});

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_redpanda_roles_request req;
    chunked_vector<kafka::role_name_filter> filters;
    filters.push_back(kafka::role_name_filter{.name = "role-a"});
    filters.push_back(kafka::role_name_filter{.name = "does-not-exist"});
    req.data.role_name_filters = std::move(filters);

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();

    BOOST_REQUIRE_EQUAL(resp.data.error_code, kafka::error_code::none);

    auto got = roles_to_map(resp.data);
    std::map<ss::sstring, member_set> expected{
      {"role-a", {{int8_t(0), "alice"}}},
    };
    BOOST_REQUIRE(got == expected);
}
