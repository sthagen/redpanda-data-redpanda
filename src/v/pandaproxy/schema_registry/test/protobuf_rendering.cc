// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/container/flat_hash_set.h"
#include "bytes/iobuf_parser.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/compatibility_common.h"
#include "pandaproxy/schema_registry/types.h"
#include "test_utils/runfiles.h"
#include "utils/file_io.h"

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <utility>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

namespace {

struct simple_sharded_store {
    explicit simple_sharded_store()
      : store{} {
        store.start(pps::is_mutable::yes, ss::default_smp_service_group())
          .get();
    }
    ~simple_sharded_store() { store.stop().get(); }
    simple_sharded_store(const simple_sharded_store&) = delete;
    simple_sharded_store(simple_sharded_store&&) = delete;
    simple_sharded_store& operator=(const simple_sharded_store&) = delete;
    simple_sharded_store& operator=(simple_sharded_store&&) = delete;

    pps::schema_id
    insert(const pps::subject_schema& schema, pps::schema_version version) {
        const auto id = next_id++;
        store
          .upsert(
            pps::seq_marker{
              std::nullopt,
              std::nullopt,
              version,
              pps::seq_marker_key_type::schema},
            schema.share(),
            id,
            version,
            pps::is_deleted::no)
          .get();
        return id;
    }

    pps::schema_id next_id{1};
    pps::sharded_store store;
};

std::filesystem::path test_dir() {
    auto test_path = test_utils::get_runfile_path(
      "src/v/pandaproxy/schema_registry/test/testdata/protobuf");
    return {test_path.value_or(".")};
}

enum class SchemaType { input, sanitized, normalized };

std::string_view to_string_view(const SchemaType st) {
    switch (st) {
    case SchemaType::input:
        return "input";
    case SchemaType::sanitized:
        return "sanitized";
    case SchemaType::normalized:
        return "normalized";
    }
}

ss::sstring read_schema(const std::string_view test_case, const SchemaType pt) {
    const auto proto_file = fmt::format(
      "{}_{}.proto", test_case, to_string_view(pt));
    const auto proto_path = test_dir() / proto_file;
    return read_fully_to_string(proto_path).get();
};

} // namespace

std::string sanitize(
  std::string_view raw_proto,
  pps::normalize norm = pps::normalize::no,
  pps::output_format format = pps::output_format::none) {
    simple_sharded_store s;
    iobuf buf = pps::make_canonical_protobuf_schema(
                  s.store,
                  pps::subject_schema{
                    pps::subject{"foo"},
                    pps::schema_definition{
                      raw_proto, pps::schema_type::protobuf, {}}},
                  norm,
                  format)
                  .get()
                  .def()
                  .raw()();
    iobuf_parser parser{std::move(buf)};
    return parser.read_string(parser.bytes_left());
}

auto normalize(std::string_view raw_proto) {
    return sanitize(raw_proto, pps::normalize::yes);
}

class ProtoRendering : public testing::TestWithParam<std::string> {};

TEST_P(ProtoRendering, test_protobuf_rendering) {
    const auto& test_case = GetParam();
    const auto input = read_schema(test_case, SchemaType::input);

    const auto sanitized_expected = read_schema(
      test_case, SchemaType::sanitized);
    const auto sanitized_processed = sanitize(input);
    EXPECT_EQ(sanitized_expected, sanitized_processed);

    const auto normalized_expected = read_schema(
      test_case, SchemaType::normalized);
    const auto normalized_processed = normalize(input);
    EXPECT_EQ(normalized_expected, normalized_processed);

    // These are to verify that the processed schemas are valid schemas.
    // We don't want to run these if the above fail because
    // they produce lot's of visual noise.
    if (sanitized_expected == sanitized_processed) {
        EXPECT_EQ(sanitized_processed, sanitize(sanitized_processed));
    }
    if (normalized_expected == normalized_processed) {
        EXPECT_EQ(normalized_processed, normalize(normalized_processed));
    }
}

TEST_P(ProtoRendering, test_protobuf_serialized_mode) {
    const auto& test_case = GetParam();
    const auto input = read_schema(test_case, SchemaType::sanitized);

    // Validate that a round trip to serialized and
    // back results in the starting schema
    auto schema_b64 = sanitize(
      input, pps::normalize::no, pps::output_format::serialized);
    const auto schema_text = sanitize(schema_b64, pps::normalize::no);
    EXPECT_EQ(input, schema_text);
}

INSTANTIATE_TEST_SUITE_P(
  Protobuf,
  ProtoRendering,
  testing::Values(
    "empty_proto",
    "syntax_proto2",
    "syntax_proto3",
    "imports",
    "package",
    "file_options",
    "enum_proto2",
    "enum_proto3",
    // These are covering message and field
    "message_proto2",
    "message_proto3",
    "service_proto2",
    "service_proto3",
    "extension_ranges",
    "composite_extension",
    "nested_extension",
    "map_entry_hack",
    "repeated_custom_options",
    "enum_ordering",
    "message_ordering",
    "service_ordering",
    "oneof_ordering"));
