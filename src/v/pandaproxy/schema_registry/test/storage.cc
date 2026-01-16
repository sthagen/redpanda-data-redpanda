// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/storage.h"

#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/types.h"

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <optional>

namespace pandaproxy::schema_registry {

namespace {

namespace ppj = pandaproxy::json;

constexpr std::string_view schema_key_sv{
  R"({
  "keytype": "SCHEMA",
  "subject": "my-kafka-value",
  "version": 1,
  "magic": 1,
  "seq": 42,
  "node": 2
})"};
const auto expected_schema_key = schema_key{
  .seq{model::offset{42}},
  .node{model::node_id{2}},
  .sub{context_subject{"my-kafka-value"}},
  .version{schema_version{1}},
  .magic{topic_key_magic{1}}};

constexpr std::string_view schema_value_sv{
  R"({
  "subject": "my-kafka-value",
  "version": 1,
  "id": 1,
  "references": [
    {"name": "name", "subject": "subject", "version": 1}
  ],
  "schema": "{\"type\":\"string\"}",
  "deleted": true
})"};
const auto expected_schema_value = schema_value{
  .schema{
    subject{"my-kafka-value"},
    schema_definition{
      R"({"type":"string"})",
      schema_type::avro,
      {{{"name"}, subject{"subject"}, schema_version{1}}},
      {}}},
  .version{schema_version{1}},
  .id{schema_id{1}},
  .deleted = is_deleted::yes};

constexpr std::string_view config_key_sv{
  R"({
  "keytype": "CONFIG",
  "subject": null,
  "magic": 0,
  "seq": 0,
  "node": 0
})"};
const auto expected_config_key = config_key{
  .seq{model::offset{0}},
  .node{model::node_id{0}},
  .sub{},
  .magic{topic_key_magic{0}}};

constexpr std::string_view config_key_sub_sv{
  R"({
  "keytype": "CONFIG",
  "subject": "my-kafka-value",
  "magic": 0,
  "seq": 0,
  "node": 0
})"};
const auto expected_config_key_sub = config_key{
  .seq{model::offset{0}},
  .node{model::node_id{0}},
  .sub{subject{"my-kafka-value"}},
  .magic{topic_key_magic{0}}};

constexpr std::string_view config_value_sv{
  R"({
  "compatibilityLevel": "FORWARD_TRANSITIVE"
})"};
const auto expected_config_value = config_value{
  .compat = compatibility_level::forward_transitive};

constexpr std::string_view config_value_sub_sv{
  R"({
  "subject": "my-kafka-value",
  "compatibilityLevel": "FORWARD_TRANSITIVE"
})"};
const auto expected_config_value_sub = config_value{
  .compat = compatibility_level::forward_transitive,
  .sub{subject{"my-kafka-value"}}};

constexpr std::string_view delete_subject_key_sv{
  R"({
  "keytype": "DELETE_SUBJECT",
  "subject": "my-kafka-value",
  "magic": 0,
  "seq": 42,
  "node": 2
})"};
const auto expected_delete_subject_key = delete_subject_key{
  .seq{model::offset{42}},
  .node{model::node_id{2}},
  .sub{subject{"my-kafka-value"}}};

constexpr std::string_view delete_subject_value_sv{
  R"({
  "subject": "my-kafka-value"
})"};
const auto expected_delete_subject_value = delete_subject_value{
  .sub{subject{"my-kafka-value"}}};

} // namespace

TEST(StorageTest, Serde) {
    {
        auto key = ppj::impl::rjson_parse(
          schema_key_sv.data(), schema_key_handler<>{});
        EXPECT_EQ(expected_schema_key, key);

        auto str = ppj::rjson_serialize_str(expected_schema_key);
        EXPECT_EQ(str, ::json::minify(schema_key_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          schema_value_sv.data(), schema_value_handler{});
        EXPECT_EQ(expected_schema_value, val);

        auto str = ppj::rjson_serialize_str(expected_schema_value);
        EXPECT_EQ(str, ::json::minify(schema_value_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          config_key_sv.data(), config_key_handler<>{});
        EXPECT_EQ(expected_config_key, val);

        auto str = ppj::rjson_serialize_str(expected_config_key);
        EXPECT_EQ(str, ::json::minify(config_key_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          config_key_sub_sv.data(), config_key_handler<>{});
        EXPECT_EQ(expected_config_key_sub, val);

        auto str = ppj::rjson_serialize_str(expected_config_key_sub);
        EXPECT_EQ(str, ::json::minify(config_key_sub_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          config_value_sv.data(), config_value_handler<>{});
        EXPECT_EQ(expected_config_value, val);

        auto str = ppj::rjson_serialize_str(expected_config_value);
        EXPECT_EQ(str, ::json::minify(config_value_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          config_value_sub_sv.data(), config_value_handler<>{});
        EXPECT_EQ(expected_config_value_sub, val);

        auto str = ppj::rjson_serialize_str(expected_config_value_sub);
        EXPECT_EQ(str, ::json::minify(config_value_sub_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          delete_subject_key_sv.data(), delete_subject_key_handler<>{});
        EXPECT_EQ(expected_delete_subject_key, val);

        auto str = ppj::rjson_serialize_str(expected_delete_subject_key);
        EXPECT_EQ(str, ::json::minify(delete_subject_key_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          delete_subject_value_sv.data(), delete_subject_value_handler<>{});
        EXPECT_EQ(expected_delete_subject_value, val);

        auto str = ppj::rjson_serialize_str(expected_delete_subject_value);
        EXPECT_EQ(str, ::json::minify(delete_subject_value_sv));
    }
}

TEST(StorageTest, SerdeMetadata) {
    const auto make_schema = [](std::optional<std::string_view> metadata) {
        constexpr std::string_view fmt_schema{
          R"({{
  "subject": "my-kafka-value",
  "version": 1,
  "id": 1,
  {}
  "schema": "{{\"type\":\"string\"}}",
  "deleted": true
}})"};
        return fmt::format(
          fmt::runtime(fmt_schema),
          metadata.has_value() ? fmt::format(R"(  "metadata": {},)", *metadata)
                               : "");
    };
    {
        auto no_metadata = make_schema(std::nullopt);
        auto val = ppj::impl::rjson_parse(
          no_metadata.data(), schema_value_handler<>{});
        EXPECT_EQ(ppj::rjson_serialize_str(val), ::json::minify(no_metadata));
        EXPECT_FALSE(val.schema.def().meta().has_value());
    }
    {
        auto null_metadata = make_schema("null");
        auto val = ppj::impl::rjson_parse(
          null_metadata.data(), schema_value_handler<>{});
        EXPECT_FALSE(val.schema.def().meta().has_value());
    }
    {
        auto empty_metadata = make_schema("{}");
        auto val = ppj::impl::rjson_parse(
          empty_metadata.data(), schema_value_handler<>{});
        EXPECT_EQ(
          ppj::rjson_serialize_str(val), ::json::minify(empty_metadata));
        EXPECT_TRUE(val.schema.def().meta().has_value());
        EXPECT_FALSE(val.schema.def().meta()->properties.has_value());
    }
    {
        auto null_metadata_properties = make_schema(R"({
    "properties": null
  })");
        auto val = ppj::impl::rjson_parse(
          null_metadata_properties.data(), schema_value_handler<>{});
        EXPECT_TRUE(val.schema.def().meta().has_value());
        EXPECT_FALSE(val.schema.def().meta()->properties.has_value());
    }
    {
        auto empty_metadata_properties = make_schema(R"({
    "properties": {}
  })");
        auto val = ppj::impl::rjson_parse(
          empty_metadata_properties.data(), schema_value_handler<>{});
        EXPECT_EQ(
          ppj::rjson_serialize_str(val),
          ::json::minify(empty_metadata_properties));
        EXPECT_TRUE(val.schema.def().meta().has_value());
        EXPECT_TRUE(val.schema.def().meta()->properties.has_value());
        EXPECT_TRUE(val.schema.def().meta()->properties->empty());
    }
    {
        auto metadata_properties = make_schema(R"({
    "properties": {
      "key1": "value1",
      "key2": "value2"
    }
  })");
        auto val = ppj::impl::rjson_parse(
          metadata_properties.data(), schema_value_handler<>{});
        EXPECT_EQ(
          ppj::rjson_serialize_str(val), ::json::minify(metadata_properties));
        EXPECT_TRUE(val.schema.def().meta().has_value());
        EXPECT_TRUE(val.schema.def().meta()->properties.has_value());
        EXPECT_EQ(val.schema.def().meta()->properties->size(), 2);
    }
}

TEST(StorageTest, SerdeContextSubject) {
    // Test schema_key with context_subject (qualified format)
    {
        constexpr std::string_view schema_key_ctx_sv{
          R"({
  "keytype": "SCHEMA",
  "subject": ":.my-context:my-kafka-value",
  "version": 1,
  "magic": 1,
  "seq": 42,
  "node": 2
})"};
        const auto expected_schema_key_ctx = schema_key{
          .seq{model::offset{42}},
          .node{model::node_id{2}},
          .sub{context{".my-context"}, subject{"my-kafka-value"}},
          .version{schema_version{1}},
          .magic{topic_key_magic{1}}};

        auto key = ppj::impl::rjson_parse(
          schema_key_ctx_sv.data(), schema_key_handler<>{});
        EXPECT_EQ(expected_schema_key_ctx, key);

        auto str = ppj::rjson_serialize_str(expected_schema_key_ctx);
        EXPECT_EQ(str, ::json::minify(schema_key_ctx_sv));
    }

    // Test schema_value with context_subject and reference with context_subject
    {
        constexpr std::string_view schema_value_ctx_sv{
          R"({
  "subject": ":.my-context:my-kafka-value",
  "version": 1,
  "id": 1,
  "references": [
    {"name": "ref-name", "subject": ":.ref-context:ref-subject", "version": 2}
  ],
  "schema": "{\"type\":\"string\"}",
  "deleted": false
})"};
        const auto expected_schema_value_ctx = schema_value{
          .schema{
            context_subject{context{".my-context"}, subject{"my-kafka-value"}},
            schema_definition{
              R"({"type":"string"})",
              schema_type::avro,
              {{{"ref-name"},
                context_subject{
                  context{".ref-context"}, subject{"ref-subject"}},
                schema_version{2}}},
              {}}},
          .version{schema_version{1}},
          .id{schema_id{1}},
          .deleted = is_deleted::no};

        auto val = ppj::impl::rjson_parse(
          schema_value_ctx_sv.data(), schema_value_handler{});
        EXPECT_EQ(expected_schema_value_ctx, val);

        auto str = ppj::rjson_serialize_str(expected_schema_value_ctx);
        EXPECT_EQ(str, ::json::minify(schema_value_ctx_sv));
    }

    // Test delete_subject_key with context_subject
    {
        constexpr std::string_view delete_key_ctx_sv{
          R"({
  "keytype": "DELETE_SUBJECT",
  "subject": ":.del-context:my-kafka-value",
  "magic": 0,
  "seq": 42,
  "node": 2
})"};
        const auto expected_delete_key_ctx = delete_subject_key{
          .seq{model::offset{42}},
          .node{model::node_id{2}},
          .sub{context{".del-context"}, subject{"my-kafka-value"}}};

        auto key = ppj::impl::rjson_parse(
          delete_key_ctx_sv.data(), delete_subject_key_handler<>{});
        EXPECT_EQ(expected_delete_key_ctx, key);

        auto str = ppj::rjson_serialize_str(expected_delete_key_ctx);
        EXPECT_EQ(str, ::json::minify(delete_key_ctx_sv));
    }

    // Test delete_subject_value with context_subject
    {
        constexpr std::string_view delete_value_ctx_sv{
          R"({
  "subject": ":.del-context:my-kafka-value"
})"};
        const auto expected_delete_value_ctx = delete_subject_value{
          .sub{context{".del-context"}, subject{"my-kafka-value"}}};

        auto val = ppj::impl::rjson_parse(
          delete_value_ctx_sv.data(), delete_subject_value_handler<>{});
        EXPECT_EQ(expected_delete_value_ctx, val);

        auto str = ppj::rjson_serialize_str(expected_delete_value_ctx);
        EXPECT_EQ(str, ::json::minify(delete_value_ctx_sv));
    }
}

} // namespace pandaproxy::schema_registry
