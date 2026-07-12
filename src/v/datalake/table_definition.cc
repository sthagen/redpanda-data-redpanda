/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/table_definition.h"

#include "strings/utf8.h"

namespace datalake {

iceberg::struct_type
rp_base_struct_type(model::iceberg_mode::headers_config headers_cfg) {
    auto st = rp_base_desc::build();
    using hsm = model::iceberg_mode::header_schema_mode;
    switch (headers_cfg.value_type) {
    case hsm::binary:
        break;
    case hsm::string: {
        auto& list = std::get<iceberg::list_type>(
          type_field<rp_desc, "headers">(rp_struct_type(st)).type);
        type_field<header_kv_desc, "value">(
          std::get<iceberg::struct_type>(list.element_field->type))
          .type = iceberg::string_type{};
        break;
    }
    }
    return st;
}

namespace {

std::optional<iceberg::value> build_headers_value(
  const chunked_vector<model::record_header>& headers,
  model::iceberg_mode::headers_config cfg) {
    if (headers.empty()) {
        return std::nullopt;
    }
    using hsm = model::iceberg_mode::header_schema_mode;
    auto hdr_list = std::make_unique<iceberg::list_value>();
    for (const auto& hdr : headers) {
        auto key_val = hdr.key_size() >= 0
                         ? std::make_optional<iceberg::value>(
                             iceberg::string_value{hdr.key().copy()})
                         : std::nullopt;
        std::optional<iceberg::value> hdr_val;
        if (cfg.value_type == hsm::string) {
            if (hdr.value_size() >= 0) {
                hdr_val = iceberg::string_value{
                  utf8_sanitize(hdr.value().copy())};
            }
        } else {
            if (hdr.value_size() >= 0) {
                hdr_val = iceberg::binary_value{hdr.value().copy()};
            }
        }
        hdr_list->elements.emplace_back(
          header_kv_desc::build_value(
            val<"key">(std::move(key_val)), val<"value">(std::move(hdr_val))));
    }
    return hdr_list;
}

} // namespace

std::unique_ptr<iceberg::struct_value> build_rp_struct(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iceberg::value> key,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers,
  model::iceberg_mode::headers_config cfg) {
    auto headers_val = build_headers_value(headers, cfg);
    return rp_desc::build_value(
      val<"partition">(iceberg::int_value(pid)),
      val<"offset">(iceberg::long_value(o)),
      val<"timestamp">(iceberg::timestamptz_value(ts.value() * 1000)),
      val<"headers">(std::move(headers_val)),
      val<"key">(std::move(key)),
      val<"timestamp_type">(iceberg::int_value{static_cast<int32_t>(ts_t)}));
}

} // namespace datalake
