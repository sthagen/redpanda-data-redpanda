/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "serde/parquet/shredder.h"
#include "serde/parquet/value.h"

#include <gtest/gtest.h>

#include <variant>

namespace serde::parquet {
namespace {

byte_array_value make_binary_value(std::string_view b) {
    return {std::make_unique<iobuf>(iobuf::from(b))};
}

template<typename... Args>
schema_element
group_node(ss::sstring name, field_repetition_type rep_type, Args... args) {
    chunked_vector<schema_element> children;
    (children.push_back(std::move(args)), ...);
    return {
      .type = std::monostate{},
      .repetition_type = rep_type,
      .path = {std::move(name)},
      .children = std::move(children),
    };
}

schema_element leaf_node(
  ss::sstring name,
  field_repetition_type rep_type,
  physical_type ptype,
  logical_type ltype = logical_type{}) {
    return {
      .type = ptype,
      .repetition_type = rep_type,
      .path = {std::move(name)},
      .logical_type = ltype,
    };
}

template<typename... Args>
group_value record(Args... field) {
    chunked_vector<group_member> fields;
    (fields.push_back(group_member{std::move(field)}), ...);
    return fields;
}

template<typename... Args>
repeated_value list(Args... field) {
    chunked_vector<repeated_element> fields;
    (fields.push_back(repeated_element{std::move(field)}), ...);
    return fields;
}

struct value_collector {
    explicit value_collector(const schema_element& root) {
        root.for_each([this](const schema_element& node) {
            if (!node.is_leaf()) {
                // Skip inner nodes.
                return;
            }
            column_mapping[node.position] = columns.size();
            columns.emplace_back();
        });
    }

    ss::future<> operator()(shredded_value val) const {
        auto it = column_mapping.find(val.schema_element_position);
        if (it == column_mapping.end()) {
            throw std::runtime_error("invalid column index");
        }
        columns[it->second].push_back(std::move(val));
        return ss::now();
    }

    std::unordered_map<int32_t, size_t> column_mapping;
    mutable chunked_vector<chunked_vector<shredded_value>> columns;
};

MATCHER_P3(IsVal, val, r, d, "") {
    return fmt::format("{}", arg.val) == val && r == arg.rep_level()
           && d == arg.def_level();
}

} // namespace
} // namespace serde::parquet

using namespace serde::parquet;

// NOLINTNEXTLINE(*internal-linkage*)
void PrintTo(
  const chunked_vector<chunked_vector<shredded_value>>& columns,
  std::ostream* os) {
    *os << "[\n";
    for (const auto& column : columns) {
        *os << "  [\n";
        for (const auto& val : column) {
            *os << fmt::format(
              "    {{val: {}, r: {}, d: {}}}\n",
              val.val,
              val.rep_level,
              val.def_level);
        }
        *os << "  ]\n";
    }
    *os << "]\n";
}

using testing::ElementsAre;

// NOLINTBEGIN(*magic-number*)
TEST(RecordShredding, ExampleFromDremelPaper) {
    schema_element document_schema = group_node(
      "Document",
      field_repetition_type::required,
      leaf_node("DocId", field_repetition_type::required, i64_type{}),
      group_node(
        "Links",
        field_repetition_type::optional,
        leaf_node("Forward", field_repetition_type::repeated, i64_type{}),
        leaf_node("Backward", field_repetition_type::repeated, i64_type{})),
      group_node(
        "Name",
        field_repetition_type::repeated,
        group_node(
          "Language",
          field_repetition_type::repeated,
          leaf_node(
            "Code",
            field_repetition_type::required,
            byte_array_type{},
            string_type{}),
          leaf_node(
            "Country",
            field_repetition_type::optional,
            byte_array_type{},
            string_type{})),
        leaf_node(
          "Url",
          field_repetition_type::optional,
          byte_array_type{},
          string_type{})));
    group_value r1 = record(
      /*DocId*/ int64_value(10),
      /*Links*/
      record(
        /*Forward*/ list(int64_value(20), int64_value(40), int64_value(60)),
        /*Backward*/ list()),
      list(
        /*Name*/
        record(
          list(
            /*Language*/
            record(
              /*Code*/
              make_binary_value("en-us"),
              /*Country*/
              make_binary_value("us")),
            /*Language*/
            record(make_binary_value("en"), null_value())),
          /*Url*/
          make_binary_value("http://A")),
        /*Name*/
        record(
          list(),
          /*Url*/
          make_binary_value("http://B")),
        /*Name*/
        record(
          list(
            /*Language*/
            record(
              /*Code*/
              make_binary_value("en-gb"),
              /*Country*/
              make_binary_value("gb"))),
          /*Url*/
          null_value())));
    group_value r2 = record(
      /*DocId*/ int64_value(20),
      /*Links*/
      record(
        /*Forward*/ list(int64_value(80)),
        /*Backward*/ list(int64_value(10), int64_value(30))),
      /*Name*/
      list(record(
        /*Language*/
        repeated_value(),
        /*Url*/
        make_binary_value("http://C"))));
    index_schema(document_schema);
    value_collector collector(document_schema);
    shred_record(document_schema, std::move(r1), collector).get();
    shred_record(document_schema, std::move(r2), collector).get();
    EXPECT_THAT(
      collector.columns,
      ElementsAre(
        // DocId
        ElementsAre(IsVal("10", 0, 0), IsVal("20", 0, 0)),
        // Links.Forward
        ElementsAre(
          IsVal("20", 0, 2),
          IsVal("40", 1, 2),
          IsVal("60", 1, 2),
          IsVal("80", 0, 2)),
        // Links.Backward
        ElementsAre(IsVal("NULL", 0, 1), IsVal("10", 0, 2), IsVal("30", 1, 2)),
        // Name.Language.Code
        ElementsAre(
          IsVal("en-us", 0, 2),
          IsVal("en", 2, 2),
          IsVal("NULL", 1, 1),
          IsVal("en-gb", 1, 2),
          IsVal("NULL", 0, 1)),
        // Name.Language.Country
        ElementsAre(
          IsVal("us", 0, 3),
          IsVal("NULL", 2, 2),
          IsVal("NULL", 1, 1),
          IsVal("gb", 1, 3),
          IsVal("NULL", 0, 1)),
        // Name.Url
        ElementsAre(
          IsVal("http://A", 0, 2),
          IsVal("http://B", 1, 2),
          IsVal("NULL", 1, 1),
          IsVal("http://C", 0, 2))));
}

TEST(RecordShredding, ListOfStrings) {
    schema_element schema = group_node(
      "ExampleList",
      field_repetition_type::required,
      leaf_node("list", field_repetition_type::repeated, byte_array_type{}));
    group_value r = record(list(
      make_binary_value("a"), make_binary_value("b"), make_binary_value("c")));
    index_schema(schema);
    value_collector collector(schema);
    shred_record(schema, std::move(r), collector).get();
    EXPECT_THAT(
      collector.columns,
      ElementsAre(
        ElementsAre(IsVal("a", 0, 1), IsVal("b", 1, 1), IsVal("c", 1, 1))));
}

TEST(RecordShredding, LogicalMap) {
    schema_element schema = group_node(
      "ExampleMap",
      field_repetition_type::required,
      group_node(
        "map",
        field_repetition_type::repeated,
        leaf_node("key", field_repetition_type::required, byte_array_type()),
        leaf_node(
          "value", field_repetition_type::optional, byte_array_type())));
    group_value r = record(list(
      record(make_binary_value("AL"), make_binary_value("Alabama")),
      record(make_binary_value("AK"), make_binary_value("Alaska"))));
    index_schema(schema);
    value_collector collector(schema);
    shred_record(schema, std::move(r), collector).get();
    EXPECT_THAT(
      collector.columns,
      ElementsAre(
        ElementsAre(IsVal("AL", 0, 1), IsVal("AK", 1, 1)),
        ElementsAre(IsVal("Alabama", 0, 2), IsVal("Alaska", 1, 2))));
}

TEST(RecordShredding, DefinitionLevels) {
    schema_element schema = group_node(
      "ExampleDefinitionLevel",
      field_repetition_type::required,
      group_node(
        "a",
        field_repetition_type::optional,
        group_node(
          "b",
          field_repetition_type::optional,
          leaf_node("c", field_repetition_type::optional, i32_type()))));
    group_value r1 = record(record(record(int32_value(42))));
    group_value r2 = record(record(record(null_value())));
    group_value r3 = record(record(null_value()));
    group_value r4 = record(null_value());
    index_schema(schema);
    value_collector collector(schema);
    shred_record(schema, std::move(r1), collector).get();
    shred_record(schema, std::move(r2), collector).get();
    shred_record(schema, std::move(r3), collector).get();
    shred_record(schema, std::move(r4), collector).get();
    EXPECT_THAT(
      collector.columns,
      ElementsAre(ElementsAre(
        IsVal("42", 0, 3),
        IsVal("NULL", 0, 2),
        IsVal("NULL", 0, 1),
        IsVal("NULL", 0, 0))));
}

TEST(RecordShredding, RepetitionLevels) {
    schema_element schema = group_node(
      "NestedLists",
      field_repetition_type::required,
      group_node(
        "level1",
        field_repetition_type::repeated,
        leaf_node(
          "level2", field_repetition_type::repeated, byte_array_type())));
    group_value r1 = record(list(
      record(list(
        make_binary_value("a"),
        make_binary_value("b"),
        make_binary_value("c"))),
      record(list(
        make_binary_value("d"),
        make_binary_value("e"),
        make_binary_value("f"),
        make_binary_value("g")))));
    group_value r2 = record(list(
      record(list(make_binary_value("h"))),
      record(list(make_binary_value("i"), make_binary_value("j")))));
    index_schema(schema);
    value_collector collector(schema);
    shred_record(schema, std::move(r1), collector).get();
    shred_record(schema, std::move(r2), collector).get();
    EXPECT_THAT(
      collector.columns,
      ElementsAre(ElementsAre(
        IsVal("a", 0, 2),
        IsVal("b", 2, 2),
        IsVal("c", 2, 2),
        IsVal("d", 1, 2),
        IsVal("e", 2, 2),
        IsVal("f", 2, 2),
        IsVal("g", 2, 2),
        IsVal("h", 0, 2),
        IsVal("i", 1, 2),
        IsVal("j", 2, 2))));
}

TEST(RecordShredding, AddressBookExample) {
    schema_element schema = group_node(
      "AddressBook",
      field_repetition_type::required,
      leaf_node(
        "owner",
        field_repetition_type::required,
        byte_array_type(),
        string_type()),
      leaf_node(
        "ownerPhoneNumbers",
        field_repetition_type::repeated,
        byte_array_type(),
        string_type()),
      group_node(
        "contacts",
        field_repetition_type::repeated,
        leaf_node(
          "name",
          field_repetition_type::required,
          byte_array_type(),
          string_type()),
        leaf_node(
          "phoneNumber",
          field_repetition_type::optional,
          byte_array_type(),
          string_type())));
    group_value r1 = record(
      make_binary_value("Julien Le Dem"),
      list(
        make_binary_value("555 123 4567"), make_binary_value("555 666 1337")),
      list(
        record(
          make_binary_value("Dmitriy Ryaboy"),
          make_binary_value("555 987 6543")),
        record(make_binary_value("Chris Anizczyk"), null_value())));
    group_value r2 = record(make_binary_value("A. Nonymous"), list(), list());
    index_schema(schema);
    value_collector collector(schema);
    shred_record(schema, std::move(r1), collector).get();
    shred_record(schema, std::move(r2), collector).get();
    EXPECT_THAT(
      collector.columns,
      ElementsAre(
        ElementsAre(IsVal("Julien Le Dem", 0, 0), IsVal("A. Nonymous", 0, 0)),
        ElementsAre(
          IsVal("555 123 4567", 0, 1),
          IsVal("555 666 1337", 1, 1),
          IsVal("NULL", 0, 0)),
        ElementsAre(
          IsVal("Dmitriy Ryaboy", 0, 1),
          IsVal("Chris Anizczyk", 1, 1),
          IsVal("NULL", 0, 0)),
        ElementsAre(
          IsVal("555 987 6543", 0, 2),
          IsVal("NULL", 1, 1),
          IsVal("NULL", 0, 0))));
}

TEST(RecordShredding, RequiredGroupWrappedInOptionalGroup) {
    schema_element schema = group_node(
      "Root",
      field_repetition_type::required,
      group_node(
        "optional",
        field_repetition_type::optional,
        group_node(
          "required",
          field_repetition_type::required,
          leaf_node("leaf", field_repetition_type::required, i32_type()))));
    group_value r1 = record(null_value());
    group_value r2 = record(record(record(int32_value(42))));
    index_schema(schema);
    value_collector collector(schema);
    shred_record(schema, std::move(r1), collector).get();
    shred_record(schema, std::move(r2), collector).get();
    EXPECT_THAT(
      collector.columns,
      ElementsAre(ElementsAre(IsVal("NULL", 0, 0), IsVal("42", 0, 1))));
}

TEST(RecordShredding, RequiredValuesNotNullValidation) {
    schema_element schema = group_node(
      "Root",
      field_repetition_type::required,
      group_node(
        "optional",
        field_repetition_type::optional,
        group_node(
          "required",
          field_repetition_type::required,
          leaf_node("leaf", field_repetition_type::required, i32_type()))));
    group_value r1 = record(record(null_value()));
    group_value r2 = record(record(record(null_value())));
    index_schema(schema);
    value_collector collector(schema);
    EXPECT_ANY_THROW(shred_record(schema, std::move(r1), collector).get());
    EXPECT_ANY_THROW(shred_record(schema, std::move(r2), collector).get());
}

// NOLINTEND(*magic-number*)
