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

#include "proto/redpanda/core/admin/v2/security.proto.h"
#include "redpanda/admin/services/security.h"
#include "security/role.h"
#include "serde/protobuf/rpc.h"

#include <gtest/gtest.h>

namespace admin {

class SecurityServiceTest : public ::testing::Test {};

// Bring internal namespace into scope for tests
using namespace internal;

// =============================================
// Tests for validate_role_name
// =============================================

TEST_F(SecurityServiceTest, ValidateRoleNameValid) {
    // Valid role names should not throw
    EXPECT_NO_THROW(validate_role_name("admin"));
    EXPECT_NO_THROW(validate_role_name("user1"));
    EXPECT_NO_THROW(validate_role_name("my-role"));
    EXPECT_NO_THROW(validate_role_name("my_role"));
    EXPECT_NO_THROW(validate_role_name("role123"));
}

// Parameterized tests for invalid role names
struct InvalidRoleNameCase {
    ss::sstring name;
    ss::sstring test_suffix;
};

class InvalidRoleNameTest
  : public ::testing::TestWithParam<InvalidRoleNameCase> {};

TEST_P(InvalidRoleNameTest, RejectsInvalidName) {
    EXPECT_THROW(
      validate_role_name(GetParam().name),
      serde::pb::rpc::invalid_argument_exception);
}

INSTANTIATE_TEST_SUITE_P(
  InvalidNames,
  InvalidRoleNameTest,
  ::testing::Values(
    InvalidRoleNameCase{"role\nname", "newline"},
    InvalidRoleNameCase{"role\tname", "tab"},
    InvalidRoleNameCase{"role\rname", "carriage_return"},
    InvalidRoleNameCase{"\x01role", "control_char"},
    InvalidRoleNameCase{"role,name", "comma"},
    InvalidRoleNameCase{"role=name", "equals"},
    InvalidRoleNameCase{"", "empty"}),
  [](const ::testing::TestParamInfo<InvalidRoleNameCase>& info) {
      return info.param.test_suffix;
  });

// =============================================
// Tests for validate_pb_role_member
// =============================================

TEST_F(SecurityServiceTest, ValidatePbRoleMemberValid) {
    proto::admin::role_user pb_user;
    pb_user.set_name("alice");
    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    EXPECT_NO_THROW(validate_pb_role_member(pb_member));
}

TEST_F(SecurityServiceTest, ValidatePbRoleMemberWithControlCharacters) {
    proto::admin::role_user pb_user;
    pb_user.set_name("alice\nsmith");
    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    EXPECT_THROW(
      validate_pb_role_member(pb_member),
      serde::pb::rpc::invalid_argument_exception);
}

TEST_F(SecurityServiceTest, ValidatePbRoleMemberEmptyMember) {
    proto::admin::role_member pb_member;

    EXPECT_THROW(
      validate_pb_role_member(pb_member),
      serde::pb::rpc::invalid_argument_exception);
}

// =============================================
// Tests for convert_to_security_role_member
// =============================================

TEST_F(SecurityServiceTest, ConvertToSecurityRoleMemberUser) {
    proto::admin::role_user pb_user;
    pb_user.set_name("alice");

    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    auto security_member = convert_to_security_role_member(pb_member);

    EXPECT_EQ(security_member.type(), security::role_member_type::user);
    EXPECT_EQ(security_member.name(), "alice");
}

TEST_F(SecurityServiceTest, ConvertToSecurityRoleMemberEmptyMember) {
    proto::admin::role_member pb_member;

    EXPECT_THROW(
      convert_to_security_role_member(pb_member),
      serde::pb::rpc::unknown_exception);
}

// =============================================
// Tests for convert_to_security_role
// =============================================

TEST_F(SecurityServiceTest, ConvertToSecurityRoleEmpty) {
    proto::admin::role pb_role;
    pb_role.set_name("empty_role");

    auto security_role = convert_to_security_role(pb_role);

    EXPECT_TRUE(security_role.members().empty());
}

TEST_F(SecurityServiceTest, ConvertToSecurityRoleSingleMember) {
    proto::admin::role pb_role;
    pb_role.set_name("single_member_role");

    proto::admin::role_user pb_user;
    pb_user.set_name("alice");
    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    auto& members = pb_role.get_members();
    members.push_back(std::move(pb_member));

    auto security_role = convert_to_security_role(pb_role);

    EXPECT_EQ(security_role.members().size(), 1);
    auto it = security_role.members().begin();
    EXPECT_EQ(it->name(), "alice");
    EXPECT_EQ(it->type(), security::role_member_type::user);
}

TEST_F(SecurityServiceTest, ConvertToSecurityRoleMultipleMembers) {
    proto::admin::role pb_role;
    pb_role.set_name("multi_member_role");

    auto& members = pb_role.get_members();

    // Add alice
    proto::admin::role_user pb_user1;
    pb_user1.set_name("alice");
    proto::admin::role_member pb_member1;
    pb_member1.set_user(std::move(pb_user1));
    members.push_back(std::move(pb_member1));

    // Add bob
    proto::admin::role_user pb_user2;
    pb_user2.set_name("bob");
    proto::admin::role_member pb_member2;
    pb_member2.set_user(std::move(pb_user2));
    members.push_back(std::move(pb_member2));

    // Add charlie
    proto::admin::role_user pb_user3;
    pb_user3.set_name("charlie");
    proto::admin::role_member pb_member3;
    pb_member3.set_user(std::move(pb_user3));
    members.push_back(std::move(pb_member3));

    auto security_role = convert_to_security_role(pb_role);

    EXPECT_EQ(security_role.members().size(), 3);

    // Check that all members are present (order may vary due to hash set)
    std::vector<ss::sstring> member_names;
    for (const auto& member : security_role.members()) {
        member_names.push_back(ss::sstring{member.name()});
    }
    std::ranges::sort(member_names);

    EXPECT_EQ(member_names[0], "alice");
    EXPECT_EQ(member_names[1], "bob");
    EXPECT_EQ(member_names[2], "charlie");
}

// =============================================
// Tests for convert_to_pb_role_member
// =============================================

TEST_F(SecurityServiceTest, ConvertToPbRoleMemberUser) {
    security::role_member security_member{
      security::role_member_type::user, "alice"};

    auto pb_member = convert_to_pb_role_member(security_member);

    EXPECT_TRUE(pb_member.has_user());
    EXPECT_EQ(pb_member.get_user().get_name(), "alice");
}

TEST_F(SecurityServiceTest, ConvertToPbRoleMemberUnknownType) {
    security::role_member security_member{
      static_cast<security::role_member_type>(-1), "unknown"};

    EXPECT_THROW(
      convert_to_pb_role_member(security_member),
      serde::pb::rpc::internal_exception);
}

// =============================================
// Tests for convert_to_pb_role
// =============================================

TEST_F(SecurityServiceTest, ConvertToPbRoleEmpty) {
    security::role security_role{};

    auto pb_role = convert_to_pb_role("empty_role", security_role);

    EXPECT_EQ(pb_role.get_name(), "empty_role");
    EXPECT_TRUE(pb_role.get_members().empty());
}

TEST_F(SecurityServiceTest, ConvertToPbRoleSingleMember) {
    security::role::container_type members;
    members.emplace(security::role_member_type::user, "alice");
    security::role security_role{std::move(members)};

    auto pb_role = convert_to_pb_role("single_member_role", security_role);

    EXPECT_EQ(pb_role.get_name(), "single_member_role");
    EXPECT_EQ(pb_role.get_members().size(), 1);

    const auto& pb_member = pb_role.get_members()[0];
    EXPECT_TRUE(pb_member.has_user());
    EXPECT_EQ(pb_member.get_user().get_name(), "alice");
}

TEST_F(SecurityServiceTest, ConvertToPbRoleMultipleMembers) {
    security::role::container_type members;
    members.emplace(security::role_member_type::user, "alice");
    members.emplace(security::role_member_type::user, "bob");
    members.emplace(security::role_member_type::user, "charlie");
    security::role security_role{std::move(members)};

    auto pb_role = convert_to_pb_role("multi_member_role", security_role);

    EXPECT_EQ(pb_role.get_name(), "multi_member_role");
    EXPECT_EQ(pb_role.get_members().size(), 3);

    // Collect member names (order may vary due to hash set)
    std::vector<ss::sstring> member_names;
    for (const auto& pb_member : pb_role.get_members()) {
        EXPECT_TRUE(pb_member.has_user());
        member_names.push_back(pb_member.get_user().get_name());
    }
    std::ranges::sort(member_names);

    EXPECT_EQ(member_names[0], "alice");
    EXPECT_EQ(member_names[1], "bob");
    EXPECT_EQ(member_names[2], "charlie");
}

// =============================================
// Tests for round-trip conversions
// =============================================

TEST_F(SecurityServiceTest, RoundTripConversionSingleMember) {
    // Create protobuf role
    proto::admin::role pb_role_original;
    pb_role_original.set_name("test_role");

    proto::admin::role_user pb_user;
    pb_user.set_name("alice");
    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    auto& members = pb_role_original.get_members();
    members.push_back(std::move(pb_member));

    // Convert to security role
    auto security_role = convert_to_security_role(pb_role_original);

    // Convert back to protobuf
    auto pb_role_final = convert_to_pb_role("test_role", security_role);

    // Verify round-trip
    EXPECT_EQ(pb_role_final.get_name(), pb_role_original.get_name());
    EXPECT_EQ(
      pb_role_final.get_members().size(),
      pb_role_original.get_members().size());

    const auto& member_final = pb_role_final.get_members()[0];
    EXPECT_TRUE(member_final.has_user());
    EXPECT_EQ(member_final.get_user().get_name(), "alice");
}

TEST_F(SecurityServiceTest, RoundTripConversionMultipleMembers) {
    // Create protobuf role with multiple members
    proto::admin::role pb_role_original;
    pb_role_original.set_name("test_role");

    auto& members = pb_role_original.get_members();

    std::vector<ss::sstring> names = {"alice", "bob", "charlie", "dave"};
    for (const auto& name : names) {
        proto::admin::role_user pb_user;
        pb_user.set_name(ss::sstring{name});
        proto::admin::role_member pb_member;
        pb_member.set_user(std::move(pb_user));
        members.push_back(std::move(pb_member));
    }

    // Convert to security role and back
    auto security_role = convert_to_security_role(pb_role_original);
    auto pb_role_final = convert_to_pb_role("test_role", security_role);

    // Verify member count
    EXPECT_EQ(pb_role_final.get_name(), pb_role_original.get_name());
    EXPECT_EQ(
      pb_role_final.get_members().size(),
      pb_role_original.get_members().size());

    // Collect and sort names
    std::vector<ss::sstring> original_names;
    for (const auto& pb_member : pb_role_original.get_members()) {
        original_names.push_back(pb_member.get_user().get_name());
    }
    std::ranges::sort(original_names);

    std::vector<ss::sstring> final_names;
    for (const auto& pb_member : pb_role_final.get_members()) {
        EXPECT_TRUE(pb_member.has_user());
        final_names.push_back(pb_member.get_user().get_name());
    }
    std::ranges::sort(final_names);

    EXPECT_EQ(final_names, original_names);
}

TEST_F(SecurityServiceTest, RoundTripConversionEmptyRole) {
    // Create empty protobuf role
    proto::admin::role pb_role_original;
    pb_role_original.set_name("empty_role");

    // Convert to security role and back
    auto security_role = convert_to_security_role(pb_role_original);
    auto pb_role_final = convert_to_pb_role("empty_role", security_role);

    // Verify round-trip
    EXPECT_EQ(pb_role_final.get_name(), pb_role_original.get_name());
    EXPECT_TRUE(pb_role_final.get_members().empty());
    EXPECT_EQ(pb_role_original, pb_role_final);
}

// =============================================
// Tests for member deduplication
// =============================================

TEST_F(SecurityServiceTest, SecurityRoleDeduplicatesMembers) {
    // Create protobuf role with duplicate members
    proto::admin::role pb_role;
    pb_role.set_name("role_with_duplicates");

    auto& members = pb_role.get_members();

    // Add alice twice
    for (int i = 0; i < 2; ++i) {
        proto::admin::role_user pb_user;
        pb_user.set_name("alice");
        proto::admin::role_member pb_member;
        pb_member.set_user(std::move(pb_user));
        members.push_back(std::move(pb_member));
    }

    // Add bob
    proto::admin::role_user pb_user_bob;
    pb_user_bob.set_name("bob");
    proto::admin::role_member pb_member_bob;
    pb_member_bob.set_user(std::move(pb_user_bob));
    members.push_back(std::move(pb_member_bob));

    // Convert to security role
    auto security_role = convert_to_security_role(pb_role);

    // Verify deduplication
    EXPECT_EQ(security_role.members().size(), 2);

    std::vector<ss::sstring> member_names;
    for (const auto& member : security_role.members()) {
        member_names.emplace_back(member.name());
    }
    std::ranges::sort(member_names);

    EXPECT_EQ(member_names[0], "alice");
    EXPECT_EQ(member_names[1], "bob");
}

} // namespace admin
