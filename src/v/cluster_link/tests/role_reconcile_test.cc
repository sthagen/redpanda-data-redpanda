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

#include "cluster_link/role_reconcile.h"
#include "security/role.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace cluster_link {
namespace {

security::role_member user(std::string_view n) {
    return security::role_member{
      security::role_member_type::user, ss::sstring{n}};
}

security::role_with_members rwm(
  std::string_view name, std::initializer_list<security::role_member> members) {
    return security::role_with_members{
      .name = security::role_name{ss::sstring{name}},
      .role = security::role{security::role::container_type{members}}};
}

} // namespace

TEST(role_reconcile, create_only) {
    chunked_vector<security::role_with_members> src{rwm("a", {user("u1")})};
    chunked_vector<security::role_with_members> dst{};
    auto changes = reconcile_roles(std::move(src), std::move(dst));
    EXPECT_THAT(
      changes.to_create,
      testing::ElementsAre(
        testing::Field(
          &security::role_with_members::name, security::role_name{"a"})));
    EXPECT_THAT(changes.to_update, testing::IsEmpty());
    EXPECT_THAT(changes.to_delete, testing::IsEmpty());
}

TEST(role_reconcile, membership_update) {
    chunked_vector<security::role_with_members> src{
      rwm("a", {user("u1"), user("u2")})};
    chunked_vector<security::role_with_members> dst{rwm("a", {user("u1")})};
    auto changes = reconcile_roles(std::move(src), std::move(dst));
    EXPECT_THAT(changes.to_create, testing::IsEmpty());
    ASSERT_THAT(
      changes.to_update,
      testing::ElementsAre(
        testing::Field(
          &security::role_with_members::name, security::role_name{"a"})));
    EXPECT_THAT(
      changes.to_update[0].role.members(),
      testing::UnorderedElementsAre(user("u1"), user("u2")));
    EXPECT_THAT(changes.to_delete, testing::IsEmpty());
}

TEST(role_reconcile, deletion_propagation) {
    chunked_vector<security::role_with_members> src{};
    chunked_vector<security::role_with_members> dst{rwm("a", {user("u1")})};
    auto changes = reconcile_roles(std::move(src), std::move(dst));
    EXPECT_THAT(changes.to_create, testing::IsEmpty());
    EXPECT_THAT(changes.to_update, testing::IsEmpty());
    EXPECT_THAT(
      changes.to_delete, testing::ElementsAre(security::role_name{"a"}));
}

TEST(role_reconcile, no_op_when_equal) {
    chunked_vector<security::role_with_members> src{rwm("a", {user("u1")})};
    chunked_vector<security::role_with_members> dst{rwm("a", {user("u1")})};
    auto changes = reconcile_roles(std::move(src), std::move(dst));
    EXPECT_THAT(changes.to_create, testing::IsEmpty());
    EXPECT_THAT(changes.to_update, testing::IsEmpty());
    EXPECT_THAT(changes.to_delete, testing::IsEmpty());
}

TEST(role_reconcile, mixed_outcomes) {
    chunked_vector<security::role_with_members> src{
      rwm("a", {user("u1"), user("u2")}), rwm("c", {user("u3")})};
    chunked_vector<security::role_with_members> dst{
      rwm("a", {user("u1")}), rwm("b", {user("u4")})};
    auto changes = reconcile_roles(std::move(src), std::move(dst));

    EXPECT_THAT(
      changes.to_create,
      testing::ElementsAre(
        testing::Field(
          &security::role_with_members::name, security::role_name{"c"})));

    ASSERT_THAT(
      changes.to_update,
      testing::ElementsAre(
        testing::Field(
          &security::role_with_members::name, security::role_name{"a"})));
    EXPECT_THAT(
      changes.to_update[0].role.members(),
      testing::UnorderedElementsAre(user("u1"), user("u2")));

    EXPECT_THAT(
      changes.to_delete, testing::ElementsAre(security::role_name{"b"}));
}

} // namespace cluster_link
