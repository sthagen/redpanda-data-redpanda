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
#include "cloud_topics/level_one/state_update.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace experimental::cloud_topics;
using namespace experimental::cloud_topics::l1;

namespace {
const object_id oid1{uuid_t::create()};
const object_id oid2{uuid_t::create()};
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
