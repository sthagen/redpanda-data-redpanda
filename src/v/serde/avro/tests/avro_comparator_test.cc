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

#include "serde/avro/tests/avro_comparator.h"

#include <avro/Compiler.hh>
#include <avro/GenericDatum.hh>
#include <gtest/gtest.h>

#include <string>
#include <string_view>

using serde::avro::testing::compare_options;
using serde::avro::testing::generic_datum_eq;

namespace {

avro::ValidSchema compile_schema(std::string_view json_schema) {
    return avro::compileJsonSchemaFromString(std::string(json_schema));
}

TEST(AvroComparatorTest, EqualPrimitiveDatums) {
    auto res = generic_datum_eq(
      avro::GenericDatum(int32_t{12}), avro::GenericDatum(int32_t{12}));
    ASSERT_TRUE(res);
}

TEST(AvroComparatorTest, FailsOnPrimitiveMismatch) {
    auto res = generic_datum_eq(
      avro::GenericDatum(int32_t{12}), avro::GenericDatum(int32_t{13}));
    ASSERT_FALSE(res);
    ASSERT_STREQ(
      "root: int mismatch (expected: 12, actual: 13)", res.message());
}

TEST(AvroComparatorTest, FailsOnUnionBranchMismatch) {
    auto union_schema = compile_schema(
      R"(["null", {"type":"long","logicalType":"timestamp-micros"}])");
    avro::GenericDatum expected(union_schema.root());
    avro::GenericDatum actual(union_schema.root());
    expected.selectBranch(1);
    expected.value<int64_t>() = 9;
    auto res = generic_datum_eq(expected, actual, "entry.some_union");
    ASSERT_FALSE(res);
    ASSERT_STREQ(
      "entry.some_union: union branch mismatch (expected: branch=1, "
      "physical=long, logical=timestamp-micros; actual: branch=0, "
      "physical=null, logical=none)",
      res.message());
}

TEST(AvroComparatorTest, FailsOnMissingMapKey) {
    auto map_schema = compile_schema(R"({"type":"map","values":"int"})");
    avro::GenericDatum expected(map_schema.root());
    avro::GenericDatum actual(map_schema.root());

    expected.value<avro::GenericMap>().value().emplace_back(
      "k1", avro::GenericDatum(int32_t{1}));
    actual.value<avro::GenericMap>().value().emplace_back(
      "k2", avro::GenericDatum(int32_t{1}));

    auto res = generic_datum_eq(expected, actual, "root");
    ASSERT_FALSE(res);
    ASSERT_STREQ("root: key missing from actual map: 'k1'", res.message());
}

TEST(AvroComparatorTest, FailsOnNestedRecordFieldMismatch) {
    auto record_schema = compile_schema(R"({
      "type":"record",
      "name":"r1",
      "fields":[
        {"name":"f","type":"int"},
        {"name":"g","type":"string"}
      ]
    })");
    avro::GenericDatum expected(record_schema.root());
    avro::GenericDatum actual(record_schema.root());

    auto& expected_record = expected.value<avro::GenericRecord>();
    auto& actual_record = actual.value<avro::GenericRecord>();
    expected_record.fieldAt(0).value<int32_t>() = 7;
    actual_record.fieldAt(0).value<int32_t>() = 8;
    expected_record.fieldAt(1).value<std::string>() = "x";
    actual_record.fieldAt(1).value<std::string>() = "x";

    auto res = generic_datum_eq(expected, actual, "payload");
    ASSERT_FALSE(res);
    ASSERT_STREQ(
      "payload.f: int mismatch (expected: 7, actual: 8)", res.message());
}

TEST(AvroComparatorTest, FailsOnLogicalTypeMismatch) {
    auto expected_schema = compile_schema(
      R"({"type":"long","logicalType":"timestamp-micros"})");
    auto actual_schema = compile_schema(
      R"({"type":"long","logicalType":"timestamp-millis"})");

    avro::GenericDatum expected(expected_schema.root());
    avro::GenericDatum actual(actual_schema.root());
    expected.value<int64_t>() = 123;
    actual.value<int64_t>() = 123;

    auto res = generic_datum_eq(expected, actual, "entry.some_timestamp");
    ASSERT_FALSE(res);
    ASSERT_STREQ(
      "entry.some_timestamp: logical type mismatch (expected timestamp-micros, "
      "actual timestamp-millis)",
      res.message());
}

TEST(AvroComparatorTest, SubsetMatchAllowsExtraNullFields) {
    auto expected_schema = compile_schema(R"({
      "type":"record","name":"r1",
      "fields":[{"name":"f","type":"int"}]
    })");
    auto actual_schema = compile_schema(R"({
      "type":"record","name":"r1",
      "fields":[
        {"name":"f","type":"int"},
        {"name":"g","type":["null","string"],"default":null}
      ]
    })");
    avro::GenericDatum expected(expected_schema.root());
    avro::GenericDatum actual(actual_schema.root());
    expected.value<avro::GenericRecord>().fieldAt(0).value<int32_t>() = 42;
    actual.value<avro::GenericRecord>().fieldAt(0).value<int32_t>() = 42;

    auto res = generic_datum_eq(
      expected,
      actual,
      "root",
      {.extra_fields = compare_options::extra_fields_policy::allow_null});
    ASSERT_TRUE(res);
}

TEST(AvroComparatorTest, SubsetMatchRejectsExtraNonNullField) {
    auto expected_schema = compile_schema(R"({
      "type":"record","name":"r1",
      "fields":[{"name":"f","type":"int"}]
    })");
    auto actual_schema = compile_schema(R"({
      "type":"record","name":"r1",
      "fields":[
        {"name":"f","type":"int"},
        {"name":"g","type":["null","string"],"default":null}
      ]
    })");
    avro::GenericDatum expected(expected_schema.root());
    avro::GenericDatum actual(actual_schema.root());
    expected.value<avro::GenericRecord>().fieldAt(0).value<int32_t>() = 42;
    actual.value<avro::GenericRecord>().fieldAt(0).value<int32_t>() = 42;
    auto& g = actual.value<avro::GenericRecord>().fieldAt(1);
    g.selectBranch(1);
    g.value<std::string>() = "not null";

    auto res = generic_datum_eq(
      expected,
      actual,
      "root",
      {.extra_fields = compare_options::extra_fields_policy::allow_null});
    ASSERT_FALSE(res);
    ASSERT_STREQ("root.g: extra field is not null", res.message());
}

TEST(AvroComparatorTest, StrictMatchRejectsExtraNullFields) {
    auto expected_schema = compile_schema(R"({
      "type":"record","name":"r1",
      "fields":[{"name":"f","type":"int"}]
    })");
    auto actual_schema = compile_schema(R"({
      "type":"record","name":"r1",
      "fields":[
        {"name":"f","type":"int"},
        {"name":"g","type":["null","string"],"default":null}
      ]
    })");
    avro::GenericDatum expected(expected_schema.root());
    avro::GenericDatum actual(actual_schema.root());
    expected.value<avro::GenericRecord>().fieldAt(0).value<int32_t>() = 42;
    actual.value<avro::GenericRecord>().fieldAt(0).value<int32_t>() = 42;

    auto res = generic_datum_eq(expected, actual, "root");
    ASSERT_FALSE(res);
    ASSERT_STREQ(
      "root: record field count mismatch (expected 1, actual 2)",
      res.message());
}

TEST(AvroComparatorTest, MapComparisonIsOrderIndependent) {
    auto map_schema = compile_schema(R"({"type":"map","values":"int"})");
    avro::GenericDatum expected(map_schema.root());
    avro::GenericDatum actual(map_schema.root());

    auto& expected_map = expected.value<avro::GenericMap>().value();
    expected_map.emplace_back("a", avro::GenericDatum(int32_t{1}));
    expected_map.emplace_back("b", avro::GenericDatum(int32_t{2}));

    auto& actual_map = actual.value<avro::GenericMap>().value();
    actual_map.emplace_back("b", avro::GenericDatum(int32_t{2}));
    actual_map.emplace_back("a", avro::GenericDatum(int32_t{1}));

    ASSERT_TRUE(generic_datum_eq(expected, actual, "root"));
}

TEST(AvroComparatorTest, FailsOnDuplicateKeyInExpectedMap) {
    auto map_schema = compile_schema(R"({"type":"map","values":"int"})");
    avro::GenericDatum expected(map_schema.root());
    avro::GenericDatum actual(map_schema.root());

    auto& expected_map = expected.value<avro::GenericMap>().value();
    expected_map.emplace_back("k", avro::GenericDatum(int32_t{1}));
    expected_map.emplace_back("k", avro::GenericDatum(int32_t{2}));

    auto& actual_map = actual.value<avro::GenericMap>().value();
    actual_map.emplace_back("a", avro::GenericDatum(int32_t{1}));
    actual_map.emplace_back("b", avro::GenericDatum(int32_t{2}));

    auto res = generic_datum_eq(expected, actual, "root");
    ASSERT_FALSE(res);
    ASSERT_STREQ("root: duplicate key in expected map: 'k'", res.message());
}

TEST(AvroComparatorTest, FailsOnDuplicateKeyInActualMap) {
    auto map_schema = compile_schema(R"({"type":"map","values":"int"})");
    avro::GenericDatum expected(map_schema.root());
    avro::GenericDatum actual(map_schema.root());

    auto& expected_map = expected.value<avro::GenericMap>().value();
    expected_map.emplace_back("a", avro::GenericDatum(int32_t{1}));
    expected_map.emplace_back("b", avro::GenericDatum(int32_t{2}));

    auto& actual_map = actual.value<avro::GenericMap>().value();
    actual_map.emplace_back("k", avro::GenericDatum(int32_t{1}));
    actual_map.emplace_back("k", avro::GenericDatum(int32_t{2}));

    auto res = generic_datum_eq(expected, actual, "root");
    ASSERT_FALSE(res);
    ASSERT_STREQ("root: duplicate key in actual map: 'k'", res.message());
}

} // anonymous namespace
