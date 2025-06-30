/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/simple_metastore.h"
#include "cloud_topics/types.h"

#include <gtest/gtest.h>

using namespace experimental::cloud_topics;
using namespace experimental::cloud_topics::l1;

namespace {

const object_id oid1{uuid_t::create()};
const object_id oid2{uuid_t::create()};
const object_id oid3{uuid_t::create()};

const std::string_view tid_a = "deadbeef-aaaa-0000-0000-000000000000/0";
const std::string_view tid_b = "deadbeef-bbbb-0000-0000-000000000000/0";
const std::string_view tid_c = "deadbeef-cccc-0000-0000-000000000000/0";

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}

using om_list_t = chunked_vector<metastore::object_metadata>;
class om_builder {
public:
    om_builder(object_id oid, size_t footer_pos) {
        out.oid = oid;
        out.footer_pos = first_byte_offset_t(footer_pos);
    }
    om_builder& add(
      std::string_view tpr_str,
      kafka::offset base_o,
      kafka::offset last_o,
      model::timestamp last_t,
      size_t first_pos,
      size_t last_pos) {
        out.ntp_metas.emplace_back(metastore::object_metadata::ntp_metadata{
          .tidp = model::topic_id_partition::from(tpr_str),
          .base_offset = base_o,
          .last_offset = last_o,
          .max_timestamp = last_t,
          .pos = first_byte_offset_t{first_pos},
          .size = byte_range_size_t{last_pos - first_pos},
        });
        return *this;
    }
    metastore::object_metadata build() { return std::move(out); }

private:
    metastore::object_metadata out;
};

} // namespace

TEST(SimpleMetastoreTest, TestGetMissingPartition) {
    simple_metastore m;
    auto offsets_res
      = m.get_offsets(model::topic_id_partition::from(tid_c)).get();
    ASSERT_FALSE(offsets_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, offsets_res.error());

    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_c), kafka::offset{0})
                     .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, get_res.error());

    auto ometa = om_builder(oid1, 200)
                   .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                   .add(tid_b, 0_o, 10_o, 2000_t, 100, 199)
                   .build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value()) << int(add_res.error());

    // We didn't add tid_c, so we should still get an error.
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(tid_c), kafka::offset{0})
                .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, get_res.error());

    // Wrong partition.
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(
                   "deadbeef-aaaa-0000-0000-000000000000/1"),
                 kafka::offset{0})
                .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, get_res.error());

    // Sanity check that we can query tid_a.
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(tid_a), kafka::offset{0})
                .get();
    ASSERT_TRUE(get_res.has_value()) << int(get_res.error());
    ASSERT_EQ(get_res->oid, oid1);
    ASSERT_EQ(get_res->footer_pos, 200);
}

TEST(SimpleMetastoreTest, TestAddWithGap) {
    simple_metastore m;
    {
        auto ometa
          = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
        ASSERT_TRUE(add_res.has_value()) << int(add_res.error());

        auto offsets_res
          = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(11_o, offsets_res->next_offset);
    }

    {
        // Add another object just past where we expect it.
        auto ometa
          = om_builder(oid2, 100).add(tid_a, 12_o, 20_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
        ASSERT_FALSE(add_res.has_value()) << int(add_res.error());
        ASSERT_EQ(metastore::errc::invalid_request, add_res.error());

        // The offsets should not be affected.
        auto offsets_res
          = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(11_o, offsets_res->next_offset);
    }

    // Now add the object right at the end, where it should be.
    auto ometa
      = om_builder(oid2, 100).add(tid_a, 11_o, 20_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value()) << int(add_res.error());

    auto offsets_res
      = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(0_o, offsets_res->start_offset);
    ASSERT_EQ(21_o, offsets_res->next_offset);
}

TEST(SimpleMetastoreTest, TestAddWithOverlap) {
    simple_metastore m;
    {
        auto ometa
          = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(
                          chunked_vector<metastore::object_metadata>::single(
                            std::move(ometa)))
                         .get();
        ASSERT_TRUE(add_res.has_value()) << int(add_res.error());
    }

    {
        // Add another object just below where we expect it.
        auto ometa
          = om_builder(oid2, 100).add(tid_a, 10_o, 20_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(
                          chunked_vector<metastore::object_metadata>::single(
                            std::move(ometa)))
                         .get();
        ASSERT_FALSE(add_res.has_value()) << int(add_res.error());
        ASSERT_EQ(metastore::errc::invalid_request, add_res.error());
    }
    {
        // Add another object that fully overlaps with what exists.
        auto ometa
          = om_builder(oid2, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
        ASSERT_FALSE(add_res.has_value()) << int(add_res.error());
        ASSERT_EQ(metastore::errc::invalid_request, add_res.error());
    }
}

TEST(SimpleMetastoreTest, TestAddPastBeginning) {
    simple_metastore m;
    // Add the first object so it doesn't start at 0.
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 1_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_FALSE(add_res.has_value()) << int(add_res.error());
    ASSERT_EQ(metastore::errc::invalid_request, add_res.error());
}

TEST(SimpleMetastoreTest, TestAddGetOffsetBasic) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid2, 100).add(tid_a, 11_o, 20_o, 2000_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid3, 100).add(tid_a, 21_o, 30_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& o : std::views::iota(0, 11)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& o : std::views::iota(11, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& o : std::views::iota(21, 31)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
}

TEST(SimpleMetastoreTest, TestAddGetOffsetBelowStart) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());

    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_a), kafka::offset{-1})
                     .get();
    ASSERT_TRUE(get_res.has_value());
    ASSERT_EQ(get_res->oid, oid1);
    ASSERT_EQ(get_res->footer_pos, 100);
}

TEST(SimpleMetastoreTest, TestAddGetOffsetOutOfRange) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());

    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_a), kafka::offset{11})
                     .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::out_of_range, get_res.error());
}

TEST(SimpleMetastoreTest, TestAddGetTimestampBasic) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 1999_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid2, 100).add(tid_a, 11_o, 20_o, 2999_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid3, 100).add(tid_a, 21_o, 30_o, 3999_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& t : {1000_t, 1999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& t : {2000_t, 2999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& t : {3000_t, 3999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
}

TEST(SimpleMetastoreTest, TestAddGetTimestampBelowStart) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());

    auto get_res
      = m.get_first_ge(model::topic_id_partition::from(tid_a), 999_t).get();
    ASSERT_TRUE(get_res.has_value());
    ASSERT_EQ(get_res->oid, oid1);
}

TEST(SimpleMetastoreTest, TestAddGetTimestampOutOfRange) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());

    auto get_res
      = m.get_first_ge(model::topic_id_partition::from(tid_a), 3000_t).get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::out_of_range, get_res.error());
}
