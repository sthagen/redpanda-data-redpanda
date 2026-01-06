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

#include <boost/test/unit_test.hpp>
#include <fmt/format.h>

#include <optional>

namespace ppj = pandaproxy::json;
namespace pps = pandaproxy::schema_registry;

constexpr std::string_view avro_schema_key_sv{
  R"({
  "keytype": "SCHEMA",
  "subject": "my-kafka-value",
  "version": 1,
  "magic": 1,
  "seq": 42,
  "node": 2
})"};
const pps::schema_key avro_schema_key{
  .seq{model::offset{42}},
  .node{model::node_id{2}},
  .sub{pps::subject{"my-kafka-value"}},
  .version{pps::schema_version{1}},
  .magic{pps::topic_key_magic{1}}};

constexpr std::string_view avro_schema_value_sv{
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
const pps::schema_value avro_schema_value{
  .schema{
    pps::subject{"my-kafka-value"},
    pps::schema_definition{
      R"({"type":"string"})",
      pps::schema_type::avro,
      {{{"name"}, pps::subject{"subject"}, pps::schema_version{1}}},
      {}}},
  .version{pps::schema_version{1}},
  .id{pps::schema_id{1}},
  .deleted = pps::is_deleted::yes};

constexpr std::string_view config_key_sv{
  R"({
  "keytype": "CONFIG",
  "subject": null,
  "magic": 0,
  "seq": 0,
  "node": 0
})"};
const pps::config_key config_key{
  .seq{model::offset{0}},
  .node{model::node_id{0}},
  .sub{},
  .magic{pps::topic_key_magic{0}}};

constexpr std::string_view config_key_sub_sv{
  R"({
  "keytype": "CONFIG",
  "subject": "my-kafka-value",
  "magic": 0,
  "seq": 0,
  "node": 0
})"};
const pps::config_key config_key_sub{
  .seq{model::offset{0}},
  .node{model::node_id{0}},
  .sub{pps::subject{"my-kafka-value"}},
  .magic{pps::topic_key_magic{0}}};

constexpr std::string_view config_value_sv{
  R"({
  "compatibilityLevel": "FORWARD_TRANSITIVE"
})"};
const pps::config_value config_value{
  .compat = pps::compatibility_level::forward_transitive};

constexpr std::string_view config_value_sub_sv{
  R"({
  "subject": "my-kafka-value",
  "compatibilityLevel": "FORWARD_TRANSITIVE"
})"};
const pps::config_value config_value_sub{

  .compat = pps::compatibility_level::forward_transitive,
  .sub{pps::subject{"my-kafka-value"}}};

constexpr std::string_view delete_subject_key_sv{
  R"({
  "keytype": "DELETE_SUBJECT",
  "subject": "my-kafka-value",
  "magic": 0,
  "seq": 42,
  "node": 2
})"};
const pps::delete_subject_key delete_subject_key{
  .seq{model::offset{42}},
  .node{model::node_id{2}},
  .sub{pps::subject{"my-kafka-value"}}};

constexpr std::string_view delete_subject_value_sv{
  R"({
  "subject": "my-kafka-value"
})"};
const pps::delete_subject_value delete_subject_value{
  .sub{pps::subject{"my-kafka-value"}}};

BOOST_AUTO_TEST_CASE(test_storage_serde) {
    {
        auto key = ppj::impl::rjson_parse(
          avro_schema_key_sv.data(), pps::schema_key_handler<>{});
        BOOST_CHECK_EQUAL(avro_schema_key, key);

        auto str = ppj::rjson_serialize_str(avro_schema_key);
        BOOST_CHECK_EQUAL(str, ::json::minify(avro_schema_key_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          avro_schema_value_sv.data(), pps::schema_value_handler{});
        BOOST_CHECK_EQUAL(avro_schema_value, val);

        auto str = ppj::rjson_serialize_str(avro_schema_value);
        BOOST_CHECK_EQUAL(str, ::json::minify(avro_schema_value_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          config_key_sv.data(), pps::config_key_handler<>{});
        BOOST_CHECK_EQUAL(config_key, val);

        auto str = ppj::rjson_serialize_str(config_key);
        BOOST_CHECK_EQUAL(str, ::json::minify(config_key_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          config_key_sub_sv.data(), pps::config_key_handler<>{});
        BOOST_CHECK_EQUAL(config_key_sub, val);

        auto str = ppj::rjson_serialize_str(config_key_sub);
        BOOST_CHECK_EQUAL(str, ::json::minify(config_key_sub_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          config_value_sv.data(), pps::config_value_handler<>{});
        BOOST_CHECK_EQUAL(config_value, val);

        auto str = ppj::rjson_serialize_str(config_value);
        BOOST_CHECK_EQUAL(str, ::json::minify(config_value_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          config_value_sub_sv.data(), pps::config_value_handler<>{});
        BOOST_CHECK_EQUAL(config_value_sub, val);

        auto str = ppj::rjson_serialize_str(config_value_sub);
        BOOST_CHECK_EQUAL(str, ::json::minify(config_value_sub_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          delete_subject_key_sv.data(), pps::delete_subject_key_handler<>{});
        BOOST_CHECK_EQUAL(delete_subject_key, val);

        auto str = ppj::rjson_serialize_str(delete_subject_key);
        BOOST_CHECK_EQUAL(str, ::json::minify(delete_subject_key_sv));
    }

    {
        auto val = ppj::impl::rjson_parse(
          delete_subject_value_sv.data(),
          pps::delete_subject_value_handler<>{});
        BOOST_CHECK_EQUAL(delete_subject_value, val);

        auto str = ppj::rjson_serialize_str(delete_subject_value);
        BOOST_CHECK_EQUAL(str, ::json::minify(delete_subject_value_sv));
    }
}

BOOST_AUTO_TEST_CASE(test_storage_serde_metadata) {
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
          no_metadata.data(), pps::schema_value_handler<>{});
        BOOST_CHECK_EQUAL(
          ppj::rjson_serialize_str(val), ::json::minify(no_metadata));
        BOOST_CHECK(!val.schema.def().meta().has_value());
    }
    {
        auto null_metadata = make_schema("null");
        auto val = ppj::impl::rjson_parse(
          null_metadata.data(), pps::schema_value_handler<>{});
        BOOST_CHECK(!val.schema.def().meta().has_value());
    }
    {
        auto empty_metadata = make_schema("{}");
        auto val = ppj::impl::rjson_parse(
          empty_metadata.data(), pps::schema_value_handler<>{});
        BOOST_CHECK_EQUAL(
          ppj::rjson_serialize_str(val), ::json::minify(empty_metadata));
        BOOST_CHECK(val.schema.def().meta().has_value());
        BOOST_CHECK(!val.schema.def().meta()->properties.has_value());
    }
    {
        auto null_metadata_properties = make_schema(R"({
    "properties": null
  })");
        auto val = ppj::impl::rjson_parse(
          null_metadata_properties.data(), pps::schema_value_handler<>{});
        BOOST_CHECK(val.schema.def().meta().has_value());
        BOOST_CHECK(!val.schema.def().meta()->properties.has_value());
    }
    {
        auto empty_metadata_properties = make_schema(R"({
    "properties": {}
  })");
        auto val = ppj::impl::rjson_parse(
          empty_metadata_properties.data(), pps::schema_value_handler<>{});
        BOOST_CHECK_EQUAL(
          ppj::rjson_serialize_str(val),
          ::json::minify(empty_metadata_properties));
        BOOST_CHECK(val.schema.def().meta().has_value());
        BOOST_CHECK(val.schema.def().meta()->properties.has_value());
        BOOST_CHECK(val.schema.def().meta()->properties->empty());
    }
    {
        auto metadata_properties = make_schema(R"({
    "properties": {
      "key1": "value1",
      "key2": "value2"
    }
  })");
        auto val = ppj::impl::rjson_parse(
          metadata_properties.data(), pps::schema_value_handler<>{});
        BOOST_CHECK_EQUAL(
          ppj::rjson_serialize_str(val), ::json::minify(metadata_properties));
        BOOST_CHECK(val.schema.def().meta().has_value());
        BOOST_CHECK(val.schema.def().meta()->properties.has_value());
        BOOST_CHECK_EQUAL(val.schema.def().meta()->properties->size(), 2);
    }
}
