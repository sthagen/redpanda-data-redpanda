// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "security/acl.h"
#include "security/tests/randoms.h"
#include "serde/rw/rw.h"

#include <gtest/gtest.h>

class acl_binding_filter_serde_evolution_fixture : public ::testing::Test {};

inline security::acl_binding_filter random_v0_acl_binding_filter() {
    // Reusing the logic from random_acl_binding_filter to generate a realistic
    // V0-serializable ACL binding filter
    auto res = tests::random_acl_binding_filter(
      tests::serialization_format::adl);
    EXPECT_EQ(
      res.pattern().subsystem(),
      security::resource_pattern_filter::resource_subsystem::kafka);
    return res;
}

inline security::acl_binding_filter random_v1_acl_binding_filter() {
    return tests::random_acl_binding_filter(tests::serialization_format::serde);
}

inline security::acl_binding_filter random_v2_acl_binding_filter() {
    // For testing, assume that V2 will just be identical to the V1 format. In
    // reality, V2 may contain new fields, which will be ignored by V1 readers.
    return tests::random_acl_binding_filter(tests::serialization_format::serde);
}

TEST_F(acl_binding_filter_serde_evolution_fixture, forward_compat_v0_to_v1) {
    auto input_v0 = random_v0_acl_binding_filter();

    iobuf buf{};
    input_v0.testing_serde_full_write_v0(buf);

    iobuf_parser parser{std::move(buf)};

    auto output_v1 = serde::read<security::acl_binding_filter>(parser);

    ASSERT_EQ(input_v0, output_v1);
}

TEST_F(acl_binding_filter_serde_evolution_fixture, backward_v1_to_v0) {
    auto input_v1 = random_v1_acl_binding_filter();

    iobuf buf{};
    serde::write(buf, input_v1);

    iobuf_parser parser{std::move(buf)};
    auto output_v0 = security::acl_binding_filter{};
    output_v0.testing_serde_full_read_v0(parser, 0);

    // NOTE: this highlights that the V0 readers will ignore the newly
    // introduced resource subsystem field.
    auto& iv1p = input_v1.pattern();
    auto& ov0p = output_v0.pattern();
    ASSERT_EQ(
      ov0p.subsystem(),
      security::resource_pattern_filter::resource_subsystem::kafka);

    ASSERT_EQ(iv1p.resource(), ov0p.resource());
    ASSERT_EQ(iv1p.name(), ov0p.name());
    ASSERT_EQ(iv1p.pattern(), ov0p.pattern());

    ASSERT_EQ(input_v1.entry(), output_v0.entry());
}

TEST_F(acl_binding_filter_serde_evolution_fixture, forward_compat_v1_to_v2) {
    auto input_v1 = random_v1_acl_binding_filter();

    iobuf buf{};
    serde::write(buf, input_v1);

    iobuf_parser parser{std::move(buf)};

    auto output_v2 = security::acl_binding_filter{};
    output_v2.testing_serde_full_read_v2(parser, 0);

    ASSERT_EQ(input_v1, output_v2);
}

TEST_F(acl_binding_filter_serde_evolution_fixture, backward_v2_to_v1) {
    auto input_v2 = random_v2_acl_binding_filter();

    iobuf buf{};
    input_v2.testing_serde_full_write_v2(buf);

    iobuf_parser parser{std::move(buf)};
    auto output_v1 = serde::read<security::acl_binding_filter>(parser);

    ASSERT_EQ(input_v2, output_v1);
}

TEST_F(acl_binding_filter_serde_evolution_fixture, round_trip_v2_to_v2) {
    auto input_v2 = random_v2_acl_binding_filter();

    iobuf buf{};
    input_v2.testing_serde_full_write_v2(buf);

    iobuf_parser parser{std::move(buf)};
    auto output_v2 = security::acl_binding_filter{};
    output_v2.testing_serde_full_read_v2(parser, 0);

    ASSERT_EQ(input_v2, output_v2);
}

TEST_F(acl_binding_filter_serde_evolution_fixture, forward_compat_v0_to_v2) {
    auto input_v0 = random_v0_acl_binding_filter();

    iobuf buf{};
    input_v0.testing_serde_full_write_v0(buf);

    iobuf_parser parser{std::move(buf)};

    auto output_v2 = security::acl_binding_filter{};
    output_v2.testing_serde_full_read_v2(parser, 0);

    ASSERT_EQ(input_v0, output_v2);
}
