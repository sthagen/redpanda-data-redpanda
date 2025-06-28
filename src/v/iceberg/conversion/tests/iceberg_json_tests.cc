/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/json_schema/frontend.h"
#include "iceberg/conversion/json_schema/ir.h"
#include "iceberg/conversion/schema_json.h"
#include "iceberg/datatypes.h"
#include "json/document.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <utility>
#include <variant>

using namespace testing;
using namespace iceberg;

namespace {
AssertionResult field_matches(
  const iceberg::nested_field_ptr& field,
  const ss::sstring& name,
  const iceberg::field_type& ft,
  iceberg::field_required required) {
    if (
      field->id != 0 || field->name != name || field->type != ft
      || field->required != required) {
        return AssertionFailure() << fmt::format(
                 "\nexpected: (id: 0, name: {}, type: {}, required: {})\n"
                 "actual  : (id: {}, name: {}, type: {}, required: {})",
                 name,
                 ft,
                 required,
                 field->id,
                 field->name,
                 field->type,
                 field->required);
    }
    return AssertionSuccess();
}

conversion_outcome<json_conversion_ir>
to_iceberg_ir(std::string_view json_str) {
    json::Document doc;
    doc.Parse(json_str.data(), json_str.size());

    try {
        auto root = iceberg::conversion::json_schema::frontend().compile(
          doc, "https://example.com/arbitrary-base-uri.json", std::nullopt);
        return iceberg::type_to_ir(root);
    } catch (const std::exception& e) {
        return conversion_exception(
          fmt::format("Failed to convert JSON schema: {}", e.what()));
    }
}

conversion_outcome<iceberg::struct_type>
to_iceberg_type(std::string_view json_str) {
    auto iceberg_ir_res = to_iceberg_ir(json_str);
    if (iceberg_ir_res.has_error()) {
        return iceberg_ir_res.error();
    }

    return iceberg::type_to_iceberg(iceberg_ir_res.value());
}

} // namespace

constexpr std::string_view schema_root_empty = R"({
  "$schema": "http://json-schema.org/draft-07/schema#"
})";
constexpr std::string_view schema_root_empty_types = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": []
})";

constexpr std::string_view schema_root_null = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "null"
})";

constexpr std::string_view schema_root_null_null = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["null", "null"]
})";

constexpr std::string_view schema_root_primitive = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "integer"
})";

constexpr std::string_view schema_root_primitive_array = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["integer"]
})";

constexpr std::string_view schema_root_primitive_array_with_null_first = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["null", "integer"]
})";

constexpr std::string_view schema_root_primitive_array_with_null_second = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["integer", "null"]
})";

constexpr std::string_view schema_root_primitive_array_or_string = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": ["integer", "string"]
})";

constexpr std::string_view nested_schema = R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://example.com/product.schema.json",
  "title": "Product",
  "description": "A product from Acme's catalog",
  "type": "object",
  "properties": {
    "productId": {
      "description": "The unique identifier for a product",
      "type": "integer"
    },
    "productName": {
      "description": "Name of the product",
      "type": "string"
    },
    "price": {
      "description": "The price of the product",
      "type": "number",
      "exclusiveMinimum": 0
    },
    "tags": {
      "description": "Tags for the product",
      "type": "array",
      "items": {
        "type": "string"
      },
      "minItems": 1,
      "uniqueItems": true
    },
    "dimensions": {
      "type": "object",
      "properties": {
        "length": {
          "type": "number"
        },
        "width": {
          "type": "number"
        },
        "height": {
          "type": "number"
        }
      },
      "required": [ "length", "width", "height" ]
    }
  },
  "required": [ "productId", "productName", "price" ]
})";

// Obfuscated schema sample from a customer.
constexpr std::string_view nested_schema_2 = R"({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "sample schema",
    "type": "object",
    "description": "sample schema description",
    "properties": {
        "key1": {
            "description": "key1 description",
            "type": [
                "string"
            ]
        },
        "key2": {
            "description": "nullable array of nullable strings",
            "items": {
                "type": [
                    "string",
                    "null"
                ]
            },
            "type": [
                "array",
                "null"
            ]
        },
        "key3": {
            "description": "nullable string",
            "type": [
                "string",
                "null"
            ]
        },
        "key4": {
            "description": "an integer",
            "type": [
                "integer"
            ]
        },
        "key5": {
            "description": "a string",
            "type": [
                "string"
            ]
        },
        "key6": {
            "description": "nullable array of objects",
            "items": {
                "type": "object",
                "properties": {
                    "key1": {
                        "type": [
                            "string",
                            "null"
                        ]
                    },
                    "key2": {
                        "format": "date-time",
                        "type": [
                            "string",
                            "null"
                        ]
                    }
                }
            },
            "type": [
                "array",
                "null"
            ]
        }
    },
    "required": [
        "key1",
        "key2",
        "key3",
        "key4",
        "key5",
        "key6"
    ]
})";

TEST(JsonSchema, Empty) {
    for (const auto& schema_str : {
           schema_root_empty,
           schema_root_empty_types,
         }) {
        SCOPED_TRACE(schema_str);

        auto result = to_iceberg_type(schema_str);
        ASSERT_TRUE(result.has_error());
        ASSERT_STREQ(
          "Unsupported JSON conversion: missing type keyword",
          result.error().what());
    }
}

TEST(JsonSchema, NullType) {
    for (const auto& schema_str : {
           schema_root_null,
           schema_root_null_null,
         }) {
        SCOPED_TRACE(schema_str);

        auto result = to_iceberg_type(schema_str);
        ASSERT_TRUE(result.has_error());
        ASSERT_STREQ(
          "Unsupported JSON conversion: missing type keyword",
          result.error().what());
    }
}

TEST(JsonSchema, PrimitiveTypes) {
    for (const auto& schema_str : {
           schema_root_primitive,
           schema_root_primitive_array,
           schema_root_primitive_array_with_null_first,
           schema_root_primitive_array_with_null_second,
         }) {
        SCOPED_TRACE(schema_str);

        auto result = to_iceberg_type(schema_str);
        ASSERT_TRUE(result.has_value()) << result.error().what();

        ASSERT_EQ(result.value().fields.size(), 1);
        ASSERT_TRUE(field_matches(
          result.value().fields[0],
          "root",
          iceberg::long_type{},
          iceberg::field_required::no));
    }
}

TEST(JsonSchema, PrimitiveTypesMixed) {
    auto result = to_iceberg_type(schema_root_primitive_array_or_string);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Type constraint is not sufficient for transforming. Types: [integer, "
      "string]",
      result.error().what());
}

TEST(JsonSchema, Nested) {
    auto result = to_iceberg_type(nested_schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 5);
}

TEST(JsonSchema, Nested2) {
    auto result = to_iceberg_type(nested_schema_2);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 6);

    EXPECT_TRUE(field_matches(
      result.value().fields[0],
      "key1",
      iceberg::string_type{},
      iceberg::field_required::no));
    EXPECT_TRUE(field_matches(
      result.value().fields[1],
      "key2",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
    EXPECT_TRUE(field_matches(
      result.value().fields[2],
      "key3",
      iceberg::string_type{},
      iceberg::field_required::no));
    EXPECT_TRUE(field_matches(
      result.value().fields[3],
      "key4",
      iceberg::long_type{},
      iceberg::field_required::no));
    EXPECT_TRUE(field_matches(
      result.value().fields[4],
      "key5",
      iceberg::string_type{},
      iceberg::field_required::no));

    {
        EXPECT_TRUE(
          result.value().fields[5]->required == iceberg::field_required::no);
        EXPECT_TRUE(std::holds_alternative<iceberg::list_type>(
          result.value().fields[5]->type));

        if (std::holds_alternative<iceberg::list_type>(
              result.value().fields[5]->type)) {
            auto& item_type = std::get<iceberg::list_type>(
                                result.value().fields[5]->type)
                                .element_field;

            auto key6_struct = iceberg::struct_type{};
            key6_struct.fields.push_back(iceberg::nested_field::create(
              0, "key1", iceberg::field_required::no, iceberg::string_type{}));
            key6_struct.fields.push_back(iceberg::nested_field::create(
              0,
              "key2",
              iceberg::field_required::no,
              iceberg::timestamptz_type{}));

            EXPECT_TRUE(field_matches(
              item_type,
              "element",
              iceberg::field_type(std::move(key6_struct)),
              iceberg::field_required::yes));
        }
    }
}

TEST(JsonSchema, ObjectWithInvalidProperty) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "name": { "type": "string" },
        "age": {}
      }
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Unsupported JSON conversion: missing type keyword",
      result.error().what());
}

TEST(JsonSchema, ObjectWithDuplicateProperty) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "name": { "type": "string" },
        "name": { "type": "integer" }
      }
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Duplicate property key: name",
      result.error().what());
}

TEST(JsonSchema, ObjectWithBooleanProperty) {
    constexpr std::string_view schema = R"({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://example.com/root.json",
    "type": "object",
    "properties": {
      "is_active": false
    }
  })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Unsupported JSON conversion: missing type keyword",
      result.error().what());
}

TEST(JsonSchema, ListWithoutItem) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array"
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Cannot convert JSON schema list type without items",
      result.error().what());
}

TEST(JsonSchema, ListWithInvalidItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": {}
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Unsupported JSON conversion: missing type keyword",
      result.error().what());
}

TEST(JsonSchema, ListWithInvalidItemsList) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [{}]
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Unsupported JSON conversion: missing type keyword",
      result.error().what());
}

TEST(JsonSchema, ListWithItem) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": { "type": "string" }
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithEmptyItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": []
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "List type items must have the type defined in JSON schema",
      result.error().what());
}

TEST(JsonSchema, ListWithItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [
          { "type": "string" },
          { "type": "string" }
      ]
    })";
    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithItemsMixed) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [
          { "type": "string" },
          { "type": "integer" }
      ]
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "List type items must have the same type, but found string and long",
      result.error().what());
}

TEST(JsonSchema, ListWithItemAndAdditionalItems) {
    // Per spec, additionalItems keyword must be ignored if items is
    // specified as an object.
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": { "type": "string" },
      "additionalItems": { "type": "integer" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithItemAndAdditionalItemsMatching) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [{ "type": "string" }],
      "additionalItems": { "type": "string" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::string_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, ListWithItemAndInvalidAdditionalItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [{ "type": "string" }],
      "additionalItems": {}
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Unsupported JSON conversion: missing type keyword",
      result.error().what());
}

TEST(JsonSchema, ListWithItemsAndConflictingAdditionalItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [
          { "type": "string" }
      ],
      "additionalItems": { "type": "boolean" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "List type items must have the same type, but found string and boolean",
      result.error().what());
}

TEST(JsonSchema, ListWithEmptyItemsAndAdditionalItems) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "array",
      "items": [],
      "additionalItems": { "type": "integer" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "root",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::long_type{}),
      iceberg::field_required::no));
}

TEST(JsonSchema, AdditionalProperties) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "additionalProperties": { "type": "string" }
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Only 'false' subschema is supported for additionalProperties keyword",
      result.error().what());
}

TEST(JsonSchema, AdditionalPropertiesFalse) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "field1": { "type": "string" }
      },
      "additionalProperties": false
    })";

    auto result = to_iceberg_type(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().fields.size(), 1);
    ASSERT_TRUE(field_matches(
      result.value().fields[0],
      "field1",
      iceberg::string_type{},
      iceberg::field_required::no));
}

TEST(JsonSchema, Format) {
    // table from format to iceberg type
    constexpr auto test_cases
      = std::to_array<std::pair<std::string_view, iceberg::primitive_type>>({
        {"date-time", iceberg::timestamptz_type{}},
        {"date", iceberg::date_type{}},
        {"time", iceberg::time_type{}},
      });

    for (const auto& [format, type] : test_cases) {
        SCOPED_TRACE(fmt::format("Testing format: {}", format));
        constexpr std::string_view schema_template = R"({{
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://example.com/root.json",
          "type": "string",
          "format": "{}"
        }})";

        auto schema = fmt::format(fmt::runtime(schema_template), format);
        auto result = to_iceberg_type(schema);
        ASSERT_TRUE(result.has_value()) << result.error().what();
        ASSERT_EQ(result.value().fields.size(), 1);
        ASSERT_TRUE(field_matches(
          result.value().fields[0], "root", type, iceberg::field_required::no));
    }
}

TEST(JsonSchema, BannedKeywords) {
    for (const auto& keyword : {
           "patternProperties",
           "dependencies",
           "$ref",
           "allOf",
           "anyOf",
           "oneOf",
           "if",
           "then",
           "else",
           "default",
         }) {
        SCOPED_TRACE(fmt::format("Testing banned keyword: {}", keyword));

        constexpr std::string_view schema_template = R"({{
          "$schema": "http://json-schema.org/draft-07/schema#",
          "$id": "https://example.com/root.json",
          "{}": {{ "type": "string" }}
        }})";

        auto schema = fmt::format(fmt::runtime(schema_template), keyword);
        auto result = to_iceberg_type(schema);
        ASSERT_TRUE(result.has_error());
        ASSERT_STREQ(
          fmt::format(
            "Failed to convert JSON schema: The {} keyword is not allowed",
            keyword)
            .c_str(),
          result.error().what());
    }
}

TEST(JsonSchema, RequiresDraft7Schema) {
    // Test without $schema keyword
    constexpr std::string_view schema_without_schema = R"({
      "$id": "https://example.com/root.json",
      "type": "integer"
    })";
    auto result = to_iceberg_type(schema_without_schema);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Schema dialect is not set (missing "
      "$schema keyword?)",
      result.error().what());

    // Test with incorrect schema version
    constexpr std::string_view schema_with_wrong_version = R"({
      "$schema": "http://json-schema.org/draft-04/schema#",
      "$id": "https://example.com/root.json",
      "type": "integer"
    })";
    result = to_iceberg_type(schema_with_wrong_version);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Unsupported JSON Schema feature: "
      "Unsupported JSON Schema dialect: "
      "http://json-schema.org/draft-04/schema#",
      result.error().what());

    // Test with correct schema version
    constexpr std::string_view schema_with_correct_version = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "integer"
    })";
    result = to_iceberg_type(schema_with_correct_version);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    // Test with nested JSON Schemas
    constexpr std::string_view nested_schema_with_mixed_versions = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/nested.json",
      "type": "object",
      "properties": {
        "field1": {
          "$schema": "http://json-schema.org/draft-04/schema#",
          "type": "string"
        },
        "field2": { "type": "integer" }
      }
    })";

    result = to_iceberg_type(nested_schema_with_mixed_versions);
    ASSERT_TRUE(result.has_error());
    ASSERT_STREQ(
      "Failed to convert JSON schema: Unsupported JSON Schema feature: "
      "Unsupported JSON Schema dialect: "
      "http://json-schema.org/draft-04/schema#",
      result.error().what());
}

TEST(JsonConversionIr, DeepCopy) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "field1": { "type": "string" },
        "field2": { "type": "integer" }
      }
    })";

    auto result = to_iceberg_ir(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();
    auto ir2 = result.value();

    auto& v = std::get<struct_type>(result.value().root()).fields[0];
    v->name = "field1_modified";

    ASSERT_EQ(std::get<struct_type>(ir2.root()).fields[0]->name, "field1");
}

TEST(JsonConversionIr, StructFieldIndex) {
    constexpr std::string_view schema = R"({
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id": "https://example.com/root.json",
      "type": "object",
      "properties": {
        "field1": { "type": "string" },
        "field2": { "type": "integer" }
      }
    })";

    auto result = to_iceberg_ir(schema);
    ASSERT_TRUE(result.has_value()) << result.error().what();

    ASSERT_EQ(result.value().struct_field_index().at("field1").field_pos, 0);
    ASSERT_EQ(result.value().struct_field_index().at("field2").field_pos, 1);
}
