/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "container/chunked_vector.h"
#include "datalake/record_translator.h"
#include "datalake/table_definition.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/record.h"

#include <gtest/gtest.h>

namespace datalake {
namespace {

using hsm = model::iceberg_mode::header_schema_mode;
using headers_config = model::iceberg_mode::headers_config;

// Build a model::record_header from plain string key and value.
model::record_header
make_header(std::string_view key, std::optional<std::string_view> value) {
    iobuf key_buf;
    key_buf.append(key.data(), key.size());
    iobuf val_buf;
    if (value) {
        val_buf.append(value->data(), value->size());
    }
    return model::record_header{
      static_cast<int32_t>(key.size()),
      std::move(key_buf),
      value ? static_cast<int32_t>(value->size()) : -1,
      std::move(val_buf)};
}

// Extract the headers list value from an rp struct (as returned by
// build_rp_struct). "headers" is at index 3 in rp_desc.
const iceberg::list_value& get_headers_list(const iceberg::struct_value& rp) {
    const auto& hdr_opt = rp.fields[3];
    EXPECT_TRUE(hdr_opt.has_value());
    return *std::get<std::unique_ptr<iceberg::list_value>>(*hdr_opt);
}

// Extract the header kv struct from a list element.
const iceberg::struct_value&
get_kv_struct(const iceberg::list_value& list, size_t idx) {
    return *std::get<std::unique_ptr<iceberg::struct_value>>(
      *list.elements[idx]);
}

// ---- rp_base_struct_type ------------------------------------------------

TEST(RpBaseStructType, BinaryConfigProducesBinaryHeaderValueType) {
    auto st = rp_base_struct_type({});
    auto& rp_type = rp_struct_type(st);
    // headers field is index 3
    auto& hdr_field = *rp_type.fields[3];
    auto& list_type = std::get<iceberg::list_type>(hdr_field.type);
    auto& kv_type = std::get<iceberg::struct_type>(
      list_type.element_field->type);
    // value field is index 1 in kv struct.
    // field_type = variant<primitive_type, struct_type, ...>; check both
    // levels.
    const auto& val_field_type = kv_type.fields[1]->type;
    ASSERT_TRUE(
      std::holds_alternative<iceberg::primitive_type>(val_field_type));
    EXPECT_TRUE(
      std::holds_alternative<iceberg::binary_type>(
        std::get<iceberg::primitive_type>(val_field_type)));
}

TEST(RpBaseStructType, StringConfigProducesStringHeaderValueType) {
    auto st = rp_base_struct_type({.value_type = hsm::string});
    auto& rp_type = rp_struct_type(st);
    auto& hdr_field = *rp_type.fields[3];
    auto& list_type = std::get<iceberg::list_type>(hdr_field.type);
    auto& kv_type = std::get<iceberg::struct_type>(
      list_type.element_field->type);
    const auto& val_field_type = kv_type.fields[1]->type;
    ASSERT_TRUE(
      std::holds_alternative<iceberg::primitive_type>(val_field_type));
    EXPECT_TRUE(
      std::holds_alternative<iceberg::string_type>(
        std::get<iceberg::primitive_type>(val_field_type)));
}

// ---- build_rp_struct / header values ---------------------------------------

TEST(BuildRpStruct, BinaryConfigProducesBinaryHeaderValues) {
    chunked_vector<model::record_header> headers;
    headers.push_back(make_header("k", "v"));

    auto row = build_rp_struct(
      model::partition_id{0},
      kafka::offset{0},
      std::nullopt,
      model::timestamp{0},
      model::timestamp_type::create_time,
      headers,
      {});

    const auto& list = get_headers_list(*row);
    ASSERT_EQ(list.elements.size(), 1);
    const auto& kv = get_kv_struct(list, 0);
    // value field (index 1) should be binary.
    // value = variant<primitive_value, ...>; check both levels.
    ASSERT_TRUE(kv.fields[1].has_value());
    ASSERT_TRUE(
      std::holds_alternative<iceberg::primitive_value>(*kv.fields[1]));
    EXPECT_TRUE(
      std::holds_alternative<iceberg::binary_value>(
        std::get<iceberg::primitive_value>(*kv.fields[1])));
}

TEST(BuildRpStruct, StringConfigProducesStringHeaderValues) {
    chunked_vector<model::record_header> headers;
    headers.push_back(make_header("k", "hello"));

    auto row = build_rp_struct(
      model::partition_id{0},
      kafka::offset{0},
      std::nullopt,
      model::timestamp{0},
      model::timestamp_type::create_time,
      headers,
      {.value_type = hsm::string});

    const auto& list = get_headers_list(*row);
    ASSERT_EQ(list.elements.size(), 1);
    const auto& kv = get_kv_struct(list, 0);
    ASSERT_TRUE(kv.fields[1].has_value());
    ASSERT_TRUE(
      std::holds_alternative<iceberg::primitive_value>(*kv.fields[1]));
    EXPECT_TRUE(
      std::holds_alternative<iceberg::string_value>(
        std::get<iceberg::primitive_value>(*kv.fields[1])));
}

TEST(BuildRpStruct, NullHeaderValue) {
    chunked_vector<model::record_header> headers;
    headers.push_back(make_header("k", std::nullopt));

    auto row = build_rp_struct(
      model::partition_id{0},
      kafka::offset{0},
      std::nullopt,
      model::timestamp{0},
      model::timestamp_type::create_time,
      headers,
      {.value_type = hsm::string});

    const auto& list = get_headers_list(*row);
    ASSERT_EQ(list.elements.size(), 1);
    const auto& kv = get_kv_struct(list, 0);
    // null value_size => std::nullopt in the value field
    EXPECT_FALSE(kv.fields[1].has_value());
}

// ---- UTF-8 sanitization plumbing -------------------------------------------
// Correctness of utf8_sanitize itself is covered by
// strings/tests:utf8_sanitize_test. These tests verify only that string header
// translation is wired to it.

// Helper: build a single string header value and return it as std::string.
std::string build_string_header_value(std::string_view raw_bytes) {
    chunked_vector<model::record_header> headers;
    headers.push_back(make_header("k", raw_bytes));

    auto row = build_rp_struct(
      model::partition_id{0},
      kafka::offset{0},
      std::nullopt,
      model::timestamp{0},
      model::timestamp_type::create_time,
      headers,
      {.value_type = hsm::string});

    const auto& list = get_headers_list(*row);
    const auto& kv = get_kv_struct(list, 0);
    const auto& sv = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(*kv.fields[1]));
    iobuf_parser p(sv.val.copy());
    return p.read_string(p.bytes_left());
}

TEST(StringHeaderSanitization, ValidUtf8PassesThrough) {
    EXPECT_EQ(build_string_header_value("hello \xC3\xA9"), "hello \xC3\xA9");
}

TEST(StringHeaderSanitization, InvalidBytesReplaced) {
    // bare continuation byte → U+FFFD
    EXPECT_EQ(build_string_header_value("\x80"), "\xEF\xBF\xBD");
}

// ---- record_translator::build_type with string mode -------------------------

using sm = model::iceberg_mode::schema_mode;
using key_config = model::iceberg_mode::key_config;
using value_config = model::iceberg_mode::value_config;

// Helper to check whether a field_type is a specific primitive type.
template<typename PrimitiveT>
bool is_primitive(const iceberg::field_type& ft) {
    if (!std::holds_alternative<iceberg::primitive_type>(ft)) {
        return false;
    }
    return std::holds_alternative<PrimitiveT>(
      std::get<iceberg::primitive_type>(ft));
}

TEST(RecordTranslatorBuildType, BinaryKeyProducesBinaryKeyFieldType) {
    record_translator t;
    auto rt = t.build_type(std::nullopt, std::nullopt);
    auto& key_field = type_field<rp_desc, "key">(rp_struct_type(rt.type));
    EXPECT_TRUE(is_primitive<iceberg::binary_type>(key_field.type));
}

TEST(RecordTranslatorBuildType, StringKeyProducesStringKeyFieldType) {
    record_translator t({.mode = sm::string}, {}, {});
    auto rt = t.build_type(std::nullopt, std::nullopt);
    auto& key_field = type_field<rp_desc, "key">(rp_struct_type(rt.type));
    EXPECT_TRUE(is_primitive<iceberg::string_type>(key_field.type));
}

TEST(RecordTranslatorBuildType, BinaryValueProducesBinaryValueColumn) {
    record_translator t;
    auto rt = t.build_type(std::nullopt, std::nullopt);
    // The value column is the second top-level field (after "redpanda").
    ASSERT_GE(rt.type.fields.size(), 2);
    EXPECT_TRUE(is_primitive<iceberg::binary_type>(rt.type.fields[1]->type));
}

TEST(RecordTranslatorBuildType, StringValueProducesStringValueColumn) {
    record_translator t({}, {.mode = sm::string}, {});
    auto rt = t.build_type(std::nullopt, std::nullopt);
    ASSERT_GE(rt.type.fields.size(), 2);
    EXPECT_TRUE(is_primitive<iceberg::string_type>(rt.type.fields[1]->type));
}

TEST(RecordTranslatorBuildType, StringKeyAndStringValue) {
    record_translator t({.mode = sm::string}, {.mode = sm::string}, {});
    auto rt = t.build_type(std::nullopt, std::nullopt);
    auto& key_field = type_field<rp_desc, "key">(rp_struct_type(rt.type));
    EXPECT_TRUE(is_primitive<iceberg::string_type>(key_field.type));
    ASSERT_GE(rt.type.fields.size(), 2);
    EXPECT_TRUE(is_primitive<iceberg::string_type>(rt.type.fields[1]->type));
}

// ---- record_translator::translate_data with string mode ---------------------
// translate_data returns ss::future but for non-schema modes the future
// resolves immediately (no I/O). The test runner starts a seastar reactor.

iobuf make_iobuf(std::string_view s) {
    iobuf buf;
    buf.append(s.data(), s.size());
    return buf;
}

std::string iobuf_to_string(const iobuf& buf) {
    iobuf_parser p(buf.copy());
    return p.read_string(p.bytes_left());
}

TEST(RecordTranslatorTranslateData, StringKeyValidUtf8) {
    record_translator t({.mode = sm::string}, {}, {});
    chunked_vector<model::record_header> headers;
    auto result = t.translate_data(
                     model::partition_id{0},
                     kafka::offset{0},
                     std::nullopt,
                     make_iobuf("hello"),
                     std::nullopt,
                     make_iobuf("val"),
                     model::timestamp{0},
                     model::timestamp_type::create_time,
                     headers)
                    .get();
    ASSERT_TRUE(result.has_value());
    // Key is in the redpanda system struct at index 4.
    auto& rp = rp_struct_value(result.value());
    ASSERT_TRUE(rp.fields[4].has_value());
    auto& sv = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(*rp.fields[4]));
    EXPECT_EQ(iobuf_to_string(sv.val), "hello");
}

TEST(RecordTranslatorTranslateData, StringKeyInvalidUtf8Sanitized) {
    record_translator t({.mode = sm::string}, {}, {});
    chunked_vector<model::record_header> headers;
    auto result = t.translate_data(
                     model::partition_id{0},
                     kafka::offset{0},
                     std::nullopt,
                     make_iobuf("\x80"),
                     std::nullopt,
                     make_iobuf("val"),
                     model::timestamp{0},
                     model::timestamp_type::create_time,
                     headers)
                    .get();
    ASSERT_TRUE(result.has_value());
    auto& rp = rp_struct_value(result.value());
    ASSERT_TRUE(rp.fields[4].has_value());
    auto& sv = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(*rp.fields[4]));
    EXPECT_EQ(iobuf_to_string(sv.val), "\xEF\xBF\xBD");
}

TEST(RecordTranslatorTranslateData, StringKeyNullProducesNullopt) {
    record_translator t({.mode = sm::string}, {}, {});
    chunked_vector<model::record_header> headers;
    auto result = t.translate_data(
                     model::partition_id{0},
                     kafka::offset{0},
                     std::nullopt,
                     std::nullopt,
                     std::nullopt,
                     make_iobuf("val"),
                     model::timestamp{0},
                     model::timestamp_type::create_time,
                     headers)
                    .get();
    ASSERT_TRUE(result.has_value());
    auto& rp = rp_struct_value(result.value());
    EXPECT_FALSE(rp.fields[4].has_value());
}

TEST(RecordTranslatorTranslateData, StringValueValidUtf8) {
    record_translator t({}, {.mode = sm::string}, {});
    chunked_vector<model::record_header> headers;
    auto result = t.translate_data(
                     model::partition_id{0},
                     kafka::offset{0},
                     std::nullopt,
                     make_iobuf("key"),
                     std::nullopt,
                     make_iobuf("hello"),
                     model::timestamp{0},
                     model::timestamp_type::create_time,
                     headers)
                    .get();
    ASSERT_TRUE(result.has_value());
    // Value column is the second top-level field (index 1).
    ASSERT_TRUE(result.value().fields[1].has_value());
    auto& sv = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(*result.value().fields[1]));
    EXPECT_EQ(iobuf_to_string(sv.val), "hello");
}

TEST(RecordTranslatorTranslateData, StringValueInvalidUtf8Sanitized) {
    record_translator t({}, {.mode = sm::string}, {});
    chunked_vector<model::record_header> headers;
    auto result = t.translate_data(
                     model::partition_id{0},
                     kafka::offset{0},
                     std::nullopt,
                     make_iobuf("key"),
                     std::nullopt,
                     make_iobuf("\x80"),
                     model::timestamp{0},
                     model::timestamp_type::create_time,
                     headers)
                    .get();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value().fields[1].has_value());
    auto& sv = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(*result.value().fields[1]));
    EXPECT_EQ(iobuf_to_string(sv.val), "\xEF\xBF\xBD");
}

TEST(RecordTranslatorTranslateData, StringValueNullProducesNullopt) {
    record_translator t({}, {.mode = sm::string}, {});
    chunked_vector<model::record_header> headers;
    auto result = t.translate_data(
                     model::partition_id{0},
                     kafka::offset{0},
                     std::nullopt,
                     make_iobuf("key"),
                     std::nullopt,
                     std::nullopt,
                     model::timestamp{0},
                     model::timestamp_type::create_time,
                     headers)
                    .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value().fields[1].has_value());
}

TEST(RecordTranslatorTranslateData, AllStringModesCombined) {
    record_translator t(
      {.mode = sm::string}, {.mode = sm::string}, {.value_type = hsm::string});
    chunked_vector<model::record_header> headers;
    headers.push_back(make_header("hk", "hv"));
    auto result = t.translate_data(
                     model::partition_id{0},
                     kafka::offset{0},
                     std::nullopt,
                     make_iobuf("mykey"),
                     std::nullopt,
                     make_iobuf("myval"),
                     model::timestamp{0},
                     model::timestamp_type::create_time,
                     headers)
                    .get();
    ASSERT_TRUE(result.has_value());

    // Key is string.
    auto& rp = rp_struct_value(result.value());
    ASSERT_TRUE(rp.fields[4].has_value());
    auto& key_sv = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(*rp.fields[4]));
    EXPECT_EQ(iobuf_to_string(key_sv.val), "mykey");

    // Value is string.
    ASSERT_TRUE(result.value().fields[1].has_value());
    auto& val_sv = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(*result.value().fields[1]));
    EXPECT_EQ(iobuf_to_string(val_sv.val), "myval");

    // Header value is string.
    const auto& hdr_list = get_headers_list(rp);
    ASSERT_EQ(hdr_list.elements.size(), 1);
    const auto& kv = get_kv_struct(hdr_list, 0);
    ASSERT_TRUE(kv.fields[1].has_value());
    EXPECT_TRUE(
      std::holds_alternative<iceberg::string_value>(
        std::get<iceberg::primitive_value>(*kv.fields[1])));
}

} // namespace
} // namespace datalake
