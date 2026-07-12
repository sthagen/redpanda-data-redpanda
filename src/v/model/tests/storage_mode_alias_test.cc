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

#include "model/metadata.h"

#include <gtest/gtest.h>

using model::redpanda_storage_mode;
using model::redpanda_storage_mode_tiered_impl;

// Full matrix: user input string x redpanda_storage_mode_tiered_impl -> parsed
// enum (nullopt = rejected). Only local/tiered/cloud/unset are valid mode
// values; the variant names and the internal 'tiered_cloud' spelling are
// rejected -- a variant is selected with redpanda.storage.mode.impl instead.
TEST(storage_mode_alias, from_user_string_matrix) {
    struct {
        std::string_view input;
        std::optional<redpanda_storage_mode> under_v1;
        std::optional<redpanda_storage_mode> under_v2;
    } cases[] = {
      {"local", redpanda_storage_mode::local, redpanda_storage_mode::local},
      {"tiered",
       redpanda_storage_mode::tiered,
       redpanda_storage_mode::tiered_cloud},
      {"cloud", redpanda_storage_mode::cloud, redpanda_storage_mode::cloud},
      {"unset", redpanda_storage_mode::unset, redpanda_storage_mode::unset},
      {"tiered_v1", std::nullopt, std::nullopt},
      {"tiered_v2", std::nullopt, std::nullopt},
      {"tiered_cloud", std::nullopt, std::nullopt},
      {"bogus", std::nullopt, std::nullopt},
    };
    for (const auto& c : cases) {
        EXPECT_EQ(
          model::redpanda_storage_mode_from_user_string(
            c.input, redpanda_storage_mode_tiered_impl::tiered_v1),
          c.under_v1)
          << c.input << " under tiered_v1";
        EXPECT_EQ(
          model::redpanda_storage_mode_from_user_string(
            c.input, redpanda_storage_mode_tiered_impl::tiered_v2),
          c.under_v2)
          << c.input << " under tiered_v2";
    }
}

// Both tiered variants display as 'tiered'; the variant is exposed through
// redpanda.storage.mode.impl (storage_mode_tiered_impl).
TEST(storage_mode_alias, user_name) {
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(redpanda_storage_mode::tiered),
      "tiered");
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(
        redpanda_storage_mode::tiered_cloud),
      "tiered");
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(redpanda_storage_mode::local),
      "local");
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(redpanda_storage_mode::cloud),
      "cloud");
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(redpanda_storage_mode::unset),
      "unset");
}

TEST(storage_mode_alias, storage_mode_tiered_impl) {
    EXPECT_EQ(
      model::storage_mode_tiered_impl(redpanda_storage_mode::tiered),
      redpanda_storage_mode_tiered_impl::tiered_v1);
    EXPECT_EQ(
      model::storage_mode_tiered_impl(redpanda_storage_mode::tiered_cloud),
      redpanda_storage_mode_tiered_impl::tiered_v2);
    EXPECT_EQ(
      model::storage_mode_tiered_impl(redpanda_storage_mode::local),
      std::nullopt);
    EXPECT_EQ(
      model::storage_mode_tiered_impl(redpanda_storage_mode::cloud),
      std::nullopt);
    EXPECT_EQ(
      model::storage_mode_tiered_impl(redpanda_storage_mode::unset),
      std::nullopt);
}

TEST(storage_mode_alias, storage_mode_with_tiered_impl) {
    EXPECT_EQ(
      model::storage_mode_with_tiered_impl(
        redpanda_storage_mode_tiered_impl::tiered_v1),
      redpanda_storage_mode::tiered);
    EXPECT_EQ(
      model::storage_mode_with_tiered_impl(
        redpanda_storage_mode_tiered_impl::tiered_v2),
      redpanda_storage_mode::tiered_cloud);
}

// The context-free parser keeps the static aliases and the internal spelling
// (used for cluster-config round-trips and the shadow-link sync fallback).
TEST(storage_mode_alias, from_string_static_aliases) {
    EXPECT_EQ(
      model::redpanda_storage_mode_from_string("tiered_v1"),
      redpanda_storage_mode::tiered);
    EXPECT_EQ(
      model::redpanda_storage_mode_from_string("tiered_v2"),
      redpanda_storage_mode::tiered_cloud);
    EXPECT_EQ(
      model::redpanda_storage_mode_from_string("tiered_cloud"),
      redpanda_storage_mode::tiered_cloud);
    EXPECT_EQ(
      model::redpanda_storage_mode_from_string("tiered"),
      redpanda_storage_mode::tiered);
}

// The impl name is the unambiguous spelling of every storage mode, used by
// the read-only redpanda.storage.mode.impl property.
TEST(storage_mode_alias, impl_name_round_trip) {
    struct {
        redpanda_storage_mode mode;
        std::string_view name;
    } cases[] = {
      {redpanda_storage_mode::local, "local"},
      {redpanda_storage_mode::tiered, "tiered_v1"},
      {redpanda_storage_mode::tiered_cloud, "tiered_v2"},
      {redpanda_storage_mode::cloud, "cloud"},
      {redpanda_storage_mode::unset, "unset"},
    };
    for (const auto& c : cases) {
        EXPECT_EQ(model::redpanda_storage_mode_impl_name(c.mode), c.name);
        EXPECT_EQ(
          model::redpanda_storage_mode_from_impl_string(c.name), c.mode);
    }
    // Ambiguous / internal spellings are not part of the impl vocabulary.
    EXPECT_EQ(
      model::redpanda_storage_mode_from_impl_string("tiered"), std::nullopt);
    EXPECT_EQ(
      model::redpanda_storage_mode_from_impl_string("tiered_cloud"),
      std::nullopt);
    EXPECT_EQ(
      model::redpanda_storage_mode_from_impl_string("bogus"), std::nullopt);
}

TEST(storage_mode_alias, redpanda_storage_mode_tiered_impl_round_trip) {
    for (auto m :
         {redpanda_storage_mode_tiered_impl::tiered_v1,
          redpanda_storage_mode_tiered_impl::tiered_v2}) {
        EXPECT_EQ(
          model::redpanda_storage_mode_tiered_impl_from_string(
            model::redpanda_storage_mode_tiered_impl_to_string(m)),
          m);
    }
    EXPECT_EQ(
      model::redpanda_storage_mode_tiered_impl_from_string("tiered"),
      std::nullopt);
}
