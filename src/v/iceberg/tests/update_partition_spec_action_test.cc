/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/tests/test_schemas.h"
#include "iceberg/transaction.h"

#include <gtest/gtest.h>

using namespace iceberg;

class UpdatePartitionSpecActionTest : public ::testing::Test {
public:
    // Create a simple table with no snapshots.
    table_metadata create_table() {
        auto s = schema{
          .schema_struct = std::get<struct_type>(test_nested_schema_type()),
          .schema_id = schema::id_t{0},
          .identifier_field_ids = {},
        };
        chunked_vector<schema> schemas;
        schemas.emplace_back(s.copy());

        chunked_vector<partition_spec> specs;
        specs.push_back(partition_spec{.spec_id = partition_spec::id_t{0}});

        return table_metadata{
          .format_version = format_version::v2,
          .table_uuid = uuid_t::create(),
          .location = uri("s3://foo/bar"),
          .last_sequence_number = sequence_number{0},
          .last_updated_ms = model::timestamp::now(),
          .last_column_id = s.highest_field_id().value(),
          .schemas = std::move(schemas),
          .current_schema_id = schema::id_t{0},
          .partition_specs = std::move(specs),
          .default_spec_id = partition_spec::id_t{0},
          .last_partition_id = partition_field::id_t{999},
        };
    }
};

TEST_F(UpdatePartitionSpecActionTest, TestAddSpec) {
    transaction tx(create_table());
    ASSERT_EQ(tx.table().partition_specs.size(), 1);

    using field = unresolved_partition_spec::field;
    chunked_vector<field> fields{field{
      .source_name = {"bar"},
      .transform = bucket_transform{.n = 16},
      .name = "field1",
    }};

    {
        // add a simple spec
        auto res = tx.set_partition_spec(
                       unresolved_partition_spec{
                         .fields = fields.copy(),
                       })
                     .get();
        ASSERT_FALSE(res.has_error());
        ASSERT_EQ(tx.table().partition_specs.size(), 2);
        ASSERT_EQ(tx.table().default_spec_id, 1);
        const auto* spec = tx.table().get_partition_spec(
          tx.table().default_spec_id);
        ASSERT_TRUE(spec);
        ASSERT_EQ(spec->fields.size(), 1);
        ASSERT_EQ(spec->fields[0].name, "field1");
        ASSERT_EQ(spec->fields[0].field_id, 1000);
    }

    {
        // adding exactly the same spec is a no-op
        auto res = tx.set_partition_spec(
                       unresolved_partition_spec{
                         .fields = fields.copy(),
                       })
                     .get();
        ASSERT_FALSE(res.has_error());
        ASSERT_EQ(tx.table().partition_specs.size(), 2);
        ASSERT_EQ(tx.table().default_spec_id, 1);
    }

    {
        // adding a field
        fields.push_back(
          field{
            .source_name = {"foo"},
            .transform = identity_transform{},
            .name = "field2",
          });
        auto res = tx.set_partition_spec(
                       unresolved_partition_spec{
                         .fields = fields.copy(),
                       })
                     .get();
        ASSERT_FALSE(res.has_error());
        ASSERT_EQ(tx.table().partition_specs.size(), 3);
        ASSERT_EQ(tx.table().default_spec_id, 2);
        const auto* spec = tx.table().get_partition_spec(
          tx.table().default_spec_id);
        ASSERT_TRUE(spec);
        ASSERT_EQ(spec->fields.size(), 2);
        ASSERT_EQ(spec->fields[0].name, "field1");
        ASSERT_EQ(spec->fields[0].field_id, 1000);
        ASSERT_EQ(spec->fields[1].name, "field2");
        ASSERT_EQ(spec->fields[1].field_id, 1001);
    }

    {
        // removing a field
        fields = chunked_vector<field>(std::next(fields.begin()), fields.end());
        auto res = tx.set_partition_spec(
                       unresolved_partition_spec{
                         .fields = fields.copy(),
                       })
                     .get();
        ASSERT_FALSE(res.has_error());
        ASSERT_EQ(tx.table().partition_specs.size(), 4);
        ASSERT_EQ(tx.table().default_spec_id, 3);
        const auto* spec = tx.table().get_partition_spec(
          tx.table().default_spec_id);
        ASSERT_TRUE(spec);
        ASSERT_EQ(spec->fields.size(), 1);
        ASSERT_EQ(spec->fields[0].name, "field2");
        ASSERT_EQ(spec->fields[0].field_id, 1001);
    }

    {
        // re-adding the same field (id must match the old one)
        fields.push_back(
          field{
            .source_name = {"bar"},
            .transform = bucket_transform{.n = 16},
            .name = "field1",
          });
        fields.push_back(
          field{
            .source_name = {"baz"},
            .transform = identity_transform{},
            .name = "field3",
          });
        auto res = tx.set_partition_spec(
                       unresolved_partition_spec{
                         .fields = fields.copy(),
                       })
                     .get();
        ASSERT_FALSE(res.has_error());
        ASSERT_EQ(tx.table().partition_specs.size(), 5);
        ASSERT_EQ(tx.table().default_spec_id, 4);
        const auto* spec = tx.table().get_partition_spec(
          tx.table().default_spec_id);
        ASSERT_TRUE(spec);
        ASSERT_EQ(spec->fields.size(), 3);
        ASSERT_EQ(spec->fields[1].name, "field1");
        ASSERT_EQ(spec->fields[1].field_id, 1000);
    }

    {
        // set the spec to empty and then add a field, check that
        // last_partition_id is honored.
        auto res = tx.set_partition_spec(unresolved_partition_spec{}).get();
        ASSERT_FALSE(res.has_error());
        ASSERT_EQ(tx.table().partition_specs.size(), 5);
        ASSERT_EQ(tx.table().default_spec_id, 0);

        fields.clear();
        fields.push_back(
          field{
            .source_name = {"person", "age"},
            .transform = bucket_transform{.n = 16},
            .name = "field4",
          });
        res = tx.set_partition_spec(
                  unresolved_partition_spec{
                    .fields = fields.copy(),
                  })
                .get();
        ASSERT_FALSE(res.has_error());
        ASSERT_EQ(tx.table().partition_specs.size(), 6);
        ASSERT_EQ(tx.table().default_spec_id, 5);
        const auto* spec = tx.table().get_partition_spec(
          tx.table().default_spec_id);
        ASSERT_TRUE(spec);
        ASSERT_EQ(spec->fields.size(), 1);
        ASSERT_EQ(spec->fields[0].name, "field4");
        ASSERT_EQ(tx.table().last_partition_id, 1003);
        ASSERT_EQ(spec->fields[0].field_id, 1003);
    }
}
