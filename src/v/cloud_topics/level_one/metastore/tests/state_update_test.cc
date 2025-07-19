/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

using namespace experimental::cloud_topics;
using namespace experimental::cloud_topics::l1;

namespace {
const object_id oid1 = l1::create_object_id();
const object_id oid2 = l1::create_object_id();
const object_id oid3 = l1::create_object_id();
const std::string_view tidp_a = "deadbeef-aaaa-0000-0000-000000000000/0";
const std::string_view tidp_b = "deadbeef-bbbb-0000-0000-000000000000/0";
const std::string_view tidp_c = "deadbeef-cccc-0000-0000-000000000000/0";

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}

class new_obj_builder {
public:
    new_obj_builder(object_id oid, size_t footer_pos) {
        out.oid = oid;
        out.footer_pos = footer_pos;
    }
    new_obj_builder(const new_obj_builder&) = delete;
    new_obj_builder(new_obj_builder&&) = default;
    new_object build() { return std::move(out); }

    new_obj_builder& add(
      std::string_view tpr_str,
      kafka::offset base_o,
      kafka::offset last_o,
      model::timestamp last_t,
      size_t first_pos,
      size_t last_pos) {
        auto tpr = model::topic_id_partition::from(tpr_str);
        out.extent_metas[tpr.topic_id][tpr.partition] = new_object::metadata{
          .base_offset = base_o,
          .last_offset = last_o,
          .max_timestamp = last_t,
          .filepos = first_pos,
          .len = last_pos - first_pos,
        };
        return *this;
    }

private:
    new_object out;
};

struct add_objects_builder {
public:
    add_objects_builder& add(new_object o) {
        out.new_objects.emplace_back(std::move(o));
        return *this;
    }
    add_objects_update build() { return std::move(out); }

private:
    add_objects_update out;
};

struct replace_objects_builder {
public:
    replace_objects_builder& add(new_object o) {
        out.new_objects.emplace_back(std::move(o));
        return *this;
    }
    replace_objects_update build() { return std::move(out); }

private:
    replace_objects_update out;
};
} // namespace

TEST(StateUpdateTest, TestEmptyAdd) {
    state s;
    auto empty_update = add_objects_update::build(s, {});
    EXPECT_FALSE(empty_update.has_value());
    EXPECT_EQ(empty_update.error(), "No objects requested");
}

TEST(StateUpdateTest, TestAddBasic) {
    state s;
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100)
                               .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                               .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                               .build())
                        .build();
        ASSERT_FALSE(update.new_objects.empty());
        auto res = update.apply(s);
        EXPECT_TRUE(res.has_value());
        EXPECT_EQ(2, s.topic_to_state.size());
    }
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid2, 100)
                               .add(tidp_c, 0_o, 10_o, 1999_t, 0, 99)
                               .add(tidp_b, 11_o, 20_o, 1999_t, 100, 199)
                               .build())
                        .build();
        ASSERT_FALSE(update.new_objects.empty());
        auto res = update.apply(s);
        EXPECT_TRUE(res.has_value());
        EXPECT_EQ(3, s.topic_to_state.size());
    }
}

TEST(StateUpdateTest, TestDuplicateAddSingleUpdate) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add(new_obj_builder(oid2, 100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .build();
    ASSERT_FALSE(update.new_objects.empty());
    auto res = update.can_apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()(), testing::HasSubstr("Input object breaks partition"));
}

TEST(StateUpdateTest, TestStartAfterZero) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100)
                           .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                           .build())
                    .build();
    state s;
    auto res = update.apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()(), testing::HasSubstr("Input object breaks partition"))
      << res.error();
    EXPECT_EQ(0, s.objects.size());
}

TEST(StateUpdateTest, TestDuplicateObject) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                           .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                           .build())
                    .build();
    state s;
    ASSERT_FALSE(update.new_objects.empty());
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(3, s.topic_to_state.size());
    EXPECT_EQ(1, s.objects.size());

    // Apply the exact same add_object -- it should be deduped.
    auto dupe_res = update.can_apply(s);
    EXPECT_FALSE(dupe_res.has_value());
    EXPECT_THAT(dupe_res.error()(), testing::HasSubstr("already exists"))
      << dupe_res.error();
    EXPECT_EQ(1, s.objects.size());
}

TEST(StateUpdateTest, TestDuplicateAddMultipleUpdates) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                           .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                           .build())
                    .build();
    state s;
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(3, s.topic_to_state.size());
    EXPECT_EQ(1, s.objects.size());

    // Tweak the update to overlap with the one we just applied but with a
    // different object.
    update.new_objects[0].oid = oid2;
    auto dupe_res = update.can_apply(s);
    EXPECT_FALSE(dupe_res.has_value());
    EXPECT_THAT(
      dupe_res.error()(), testing::HasSubstr("Input object breaks partition"))
      << dupe_res.error();
    EXPECT_EQ(1, s.objects.size());
}

namespace {

MATCHER_P3(MatchesRange, oid, base, last, "") {
    return arg.oid == oid && arg.base_offset == base && arg.last_offset == last;
}
} // namespace

TEST(StateUpdateTest, TestReplaceBasic) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                        .build())
                 .add(new_obj_builder(oid2, 100)
                        .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                        .add(tidp_c, 11_o, 20_o, 1999_t, 0, 99)
                        .build())
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    // Fully replace partition a, partially replace c.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid3, 100)
                            .add(tidp_a, 0_o, 20_o, 1999_t, 0, 99)
                            .add(tidp_c, 0_o, 10_o, 1999_t, 100, 199)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_TRUE(replace_res.has_value());

    // Fully replaced.
    const auto& prt_a
      = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid3, 0_o, 20_o)));

    // Not replaced.
    const auto& prt_b
      = s.partition_state(model::topic_id_partition::from(tidp_b))->get();
    EXPECT_THAT(prt_b.extents, ElementsAre(MatchesRange(oid1, 0_o, 10_o)));

    // Partially replaced.
    const auto& prt_c
      = s.partition_state(model::topic_id_partition::from(tidp_c))->get();
    EXPECT_THAT(
      prt_c.extents,
      ElementsAre(
        MatchesRange(oid3, 0_o, 10_o), MatchesRange(oid2, 11_o, 20_o)));

    EXPECT_EQ(s.objects.at(oid1).removed_data_size, 198);
    EXPECT_EQ(s.objects.at(oid2).removed_data_size, 99);
    EXPECT_EQ(s.objects.at(oid3).removed_data_size, 0);
}

TEST(StateUpdateTest, TestReplaceEmptyState) {
    state s;
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid1, 100)
                            .add(tidp_a, 0_o, 20_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex("Partition .+ not tracked by state"));
}

TEST(StateUpdateTest, TestReplaceDuplicate) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid1, 100)
                            .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex("Object .+ already exists"));
}

TEST(StateUpdateTest, TestReplaceMisaligned) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100)
                            .add(tidp_a, 0_o, 9_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex(
        "Partition .+ doesn't contain extents that span exactly"));
}

TEST(StateUpdateTest, TestReplaceBadOrdering) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    // Make the replacement overlap with itself.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100)
                            .add(tidp_a, 0_o, 7_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid3, 100)
                            .add(tidp_a, 5_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex("breaks partition .+ offset ordering"));
}

TEST(StateUpdateTest, TestEmptyReplace) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder().build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::StrEq("No objects requested"));
}
