/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/tests/test_schemas.h"

#include <gtest/gtest.h>

using namespace iceberg;

namespace {

table_metadata create_table() {
    auto s = schema{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    chunked_vector<schema> schemas;
    schemas.emplace_back(s.copy());
    partition_spec pspec{.spec_id = partition_spec::id_t{0}};
    pspec.fields.push_back(
      partition_field{
        .source_id = nested_field::id_t{2},
        .field_id = partition_field::id_t{1000},
        .name = "field1",
        .transform = identity_transform{},
      });
    return table_metadata{
      .format_version = format_version::v2,
      .table_uuid = uuid_t::create(),
      .location = uri("s3://bucket/foo/bar"),
      .last_sequence_number = sequence_number{0},
      .last_updated_ms = model::timestamp::now(),
      .last_column_id = s.highest_field_id().value(),
      .schemas = std::move(schemas),
      .current_schema_id = schema::id_t{0},
      .partition_specs = chunked_vector<partition_spec>::single(
        std::move(pspec)),
      .default_spec_id = partition_spec::id_t{0},
      .last_partition_id = partition_field::id_t{1000},
      .current_snapshot_id = snapshot_id{123456},
    };
}

} // namespace

TEST(TableRequirements, TestCheck) {
    auto md = create_table();

    auto check_res = table_requirement::check(
      table_requirement::assert_create{}, nullptr);
    ASSERT_FALSE(check_res.has_error()) << check_res.error();
    check_res = table_requirement::check(
      table_requirement::assert_create{}, &md);
    ASSERT_TRUE(check_res.has_error());

    check_res = table_requirement::check(
      table_requirement::assert_table_uuid{.uuid = md.table_uuid}, &md);
    ASSERT_FALSE(check_res.has_error()) << check_res.has_error();
    auto other_uuid = md.table_uuid;
    other_uuid.mutable_uuid().data[0] += 1;
    check_res = table_requirement::check(
      table_requirement::assert_table_uuid{.uuid = other_uuid}, &md);
    ASSERT_TRUE(check_res.has_error());

    check_res = table_requirement::check(
      table_requirement::last_assigned_field_match{
        .last_assigned_field_id = md.last_column_id},
      &md);
    ASSERT_FALSE(check_res.has_error()) << check_res.has_error();
    check_res = table_requirement::check(
      table_requirement::last_assigned_field_match{
        .last_assigned_field_id = nested_field::id_t{0}},
      &md);
    ASSERT_TRUE(check_res.has_error());

    check_res = table_requirement::check(
      table_requirement::assert_current_schema_id{
        .current_schema_id = schema::id_t{0}},
      &md);
    ASSERT_FALSE(check_res.has_error()) << check_res.has_error();
    check_res = table_requirement::check(
      table_requirement::assert_current_schema_id{
        .current_schema_id = schema::id_t{1}},
      &md);
    ASSERT_TRUE(check_res.has_error());

    check_res = table_requirement::check(
      table_requirement::assert_last_assigned_partition_id{
        .last_assigned_partition_id = partition_field::id_t{1000}},
      &md);
    ASSERT_FALSE(check_res.has_error()) << check_res.has_error();
    check_res = table_requirement::check(
      table_requirement::assert_last_assigned_partition_id{
        .last_assigned_partition_id = partition_field::id_t{1001}},
      &md);
    ASSERT_TRUE(check_res.has_error());

    check_res = table_requirement::check(
      table_requirement::assert_default_spec_id{
        .default_spec_id = partition_spec::id_t{0}},
      &md);
    ASSERT_FALSE(check_res.has_error()) << check_res.has_error();
    check_res = table_requirement::check(
      table_requirement::assert_default_spec_id{
        .default_spec_id = partition_spec::id_t{1}},
      &md);
    ASSERT_TRUE(check_res.has_error());

    check_res = table_requirement::check(
      table_requirement::assert_ref_snapshot_id{
        .ref = "main", .snapshot_id = snapshot_id{123456}},
      &md);
    ASSERT_FALSE(check_res.has_error()) << check_res.has_error();
    check_res = table_requirement::check(
      table_requirement::assert_ref_snapshot_id{
        .ref = "other", .snapshot_id = std::nullopt},
      &md);
    ASSERT_FALSE(check_res.has_error()) << check_res.has_error();
    check_res = table_requirement::check(
      table_requirement::assert_ref_snapshot_id{
        .ref = "main", .snapshot_id = std::nullopt},
      &md);
    ASSERT_TRUE(check_res.has_error());
}
