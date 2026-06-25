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

} // namespace
} // namespace datalake
