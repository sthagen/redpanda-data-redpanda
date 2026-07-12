/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/record_translator.h"

#include "absl/container/flat_hash_set.h"
#include "base/vlog.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/table_definition.h"
#include "iceberg/compatibility_utils.h"
#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/values_avro.h"
#include "iceberg/conversion/values_json.h"
#include "iceberg/conversion/values_protobuf.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "strings/utf8.h"

namespace datalake {

namespace {

struct value_translating_visitor {
    // Buffer ready to be parsed, e.g. no schema ID or protobuf offsets.
    iobuf parsable_buf;
    const iceberg::field_type& type;

    ss::future<iceberg::optional_value_outcome>
    operator()(const google::protobuf::Descriptor& d) {
        return iceberg::deserialize_protobuf(std::move(parsable_buf), d);
    }
    ss::future<iceberg::optional_value_outcome>
    operator()(const avro::ValidSchema& s) {
        auto value = co_await iceberg::deserialize_avro(
          std::move(parsable_buf), s);
        if (value.has_error()) {
            co_return iceberg::optional_value_outcome(value.error());
        }
        co_return std::move(value.value());
    }

    ss::future<iceberg::optional_value_outcome>
    operator()(const iceberg::json_conversion_ir& s) {
        auto value = co_await iceberg::deserialize_json(
          std::move(parsable_buf), s);
        if (value.has_error()) {
            co_return iceberg::optional_value_outcome(value.error());
        }
        co_return std::move(value.value());
    }
};

std::optional<size_t> get_redpanda_idx(const iceberg::struct_type& val_type) {
    for (size_t i = 0; i < val_type.fields.size(); ++i) {
        if (val_type.fields[i]->name == rp_struct_name) {
            return i;
        }
    }
    return std::nullopt;
}

using schema_mode = model::iceberg_mode::schema_mode;

bool is_schema_mode(schema_mode mode) {
    switch (mode) {
    case schema_mode::binary:
    case schema_mode::string:
        return false;
    case schema_mode::schema_id_prefix:
    case schema_mode::schema_latest:
        return true;
    }
}

} // namespace

// The various schema languages differ significantly in their semantics
// and best practices around required fields, and Iceberg has its own.
// By forcing all schema fields to non-required, we provide a maximally
// permissive allowance for schema evolution which is certainly a
// superset of what any particular schema language allows.
// TODO(iceberg): this behavior could be made configurable
//
// Keys must be marked as required per the Iceberg spec:
// https://iceberg.apache.org/spec/#nested-types.
// This approach of storing the `key` fields in a set below is
// guaranteed to work because `iceberg::for_each_field()` is a depth
// first traversal of the fields, i.e., the parent `map` field will be
// visited before the child `key` field.
void relax_field_requirements(iceberg::struct_type& st) {
    absl::flat_hash_set<iceberg::nested_field*> map_keys;
    std::ignore = iceberg::for_each_field(
      st, [&map_keys](iceberg::nested_field* f) {
          f->required = map_keys.contains(f) ? iceberg::field_required::yes
                                             : iceberg::field_required::no;
          if (std::holds_alternative<iceberg::map_type>(f->type)) {
              auto& kv = std::get<iceberg::map_type>(f->type);
              map_keys.insert(kv.key_field.get());
          }
      });
}

record_type record_translator::build_type(
  std::optional<shared_resolved_type_t> key_type,
  std::optional<shared_resolved_type_t> val_type) {
    auto ret_type = rp_base_struct_type(_headers_cfg);

    if (key_type.has_value()) {
        auto key_struct = std::get<iceberg::struct_type>(
          iceberg::make_copy(key_type.value()->type));
        relax_field_requirements(key_struct);
        type_field<rp_desc, "key">(rp_struct_type(ret_type)).type = std::move(
          key_struct);
    } else if (_key_cfg.mode == schema_mode::string) {
        type_field<rp_desc, "key">(rp_struct_type(ret_type)).type
          = iceberg::string_type{};
    }

    using value_layout = model::iceberg_mode::value_layout;

    std::optional<schema_identifier> val_id;
    if (val_type.has_value()) {
        val_id = val_type.value()->id;
        auto struct_type = std::get<iceberg::struct_type>(
          iceberg::make_copy(val_type.value()->type));
        relax_field_requirements(struct_type);
        switch (_val_cfg.layout) {
        case value_layout::nested:
            ret_type.fields.emplace_back(
              iceberg::nested_field::create(
                rp_base_next_field_id,
                "value",
                iceberg::field_required::no,
                std::move(struct_type)));
            break;
        case value_layout::flat:
            for (auto& field : struct_type.fields) {
                if (field->name == rp_struct_name) {
                    // To avoid collisions, move user fields named "redpanda"
                    // into the nested "redpanda" system field.
                    auto& system_fields = rp_struct_type(ret_type);
                    system_fields.fields.emplace_back(
                      iceberg::nested_field::create(
                        rp_base_next_field_id,
                        "data",
                        field->required,
                        std::move(field->type)));
                    continue;
                }
                ret_type.fields.emplace_back(std::move(field));
            }
            break;
        }
    } else {
        iceberg::field_type val_field_type
          = _val_cfg.mode == schema_mode::string
              ? iceberg::field_type{iceberg::string_type{}}
              : iceberg::field_type{iceberg::binary_type{}};
        ret_type.fields.emplace_back(
          iceberg::nested_field::create(
            rp_base_next_field_id,
            "value",
            iceberg::field_required::no,
            std::move(val_field_type)));
    }

    return record_type{
      .comps = record_schema_components{
          .key_identifier = key_type
                              ? std::make_optional(key_type.value()->id)
                              : std::nullopt,
          .val_identifier = std::move(val_id),
      },
      .type = std::move(ret_type),
    };
}

ss::future<checked<iceberg::struct_value, record_translator::errc>>
record_translator::translate_data(
  model::partition_id pid,
  kafka::offset o,
  std::optional<shared_resolved_type_t> key_type,
  std::optional<iobuf> parsable_key,
  std::optional<shared_resolved_type_t> val_type,
  std::optional<iobuf> parsable_val,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers) {
    if (key_type.has_value() != is_schema_mode(_key_cfg.mode)) {
        vlog(
          datalake_log.error,
          "Key schema presence ({}) does not match key config mode ({})",
          key_type.has_value(),
          static_cast<int>(_key_cfg.mode));
        co_return errc::unexpected_schema;
    }
    if (val_type.has_value() != is_schema_mode(_val_cfg.mode)) {
        vlog(
          datalake_log.error,
          "Value schema presence ({}) does not match value config mode ({})",
          val_type.has_value(),
          static_cast<int>(_val_cfg.mode));
        co_return errc::unexpected_schema;
    }

    // Decode key.
    std::optional<iceberg::value> key_val;
    if (parsable_key.has_value()) {
        switch (_key_cfg.mode) {
        case schema_mode::binary:
            key_val = iceberg::binary_value{std::move(*parsable_key)};
            break;
        case schema_mode::string:
            key_val = iceberg::string_value{
              utf8_sanitize(std::move(*parsable_key))};
            break;
        case schema_mode::schema_id_prefix:
            [[fallthrough]];
        case schema_mode::schema_latest: {
            auto& resolved = *key_type.value();
            auto translated_key = co_await std::visit(
              value_translating_visitor{
                std::move(*parsable_key), resolved.type},
              resolved.schema.get_schema_ref());
            if (translated_key.has_error()) {
                vlog(
                  datalake_log.warn,
                  "Error converting key buffer: {}",
                  translated_key.error());
                co_return errc::translation_error;
            }
            key_val = std::move(translated_key.value());
        }
        }
    }

    auto ret_data = iceberg::struct_value{};
    auto system_data = build_rp_struct(
      pid, o, std::move(key_val), ts, ts_t, headers, _headers_cfg);
    ret_data.fields.emplace_back(std::move(system_data));

    // Decode value.
    if (val_type.has_value()) {
        if (!parsable_val.has_value()) {
            vlog(datalake_log.error, "Tombstones cannot be translated");
            co_return errc::translation_error;
        }
        auto& resolved = *val_type.value();
        auto translated_val = co_await std::visit(
          value_translating_visitor{std::move(*parsable_val), resolved.type},
          resolved.schema.get_schema_ref());
        if (translated_val.has_error()) {
            vlog(
              datalake_log.warn,
              "Error converting value buffer: {}",
              translated_val.error());
            co_return errc::translation_error;
        }

        auto val_struct = std::move(
          std::get<std::unique_ptr<iceberg::struct_value>>(
            translated_val.value().value()));

        using value_layout = model::iceberg_mode::value_layout;
        switch (_val_cfg.layout) {
        case value_layout::nested:
            ret_data.fields.emplace_back(
              std::make_optional<iceberg::value>(std::move(val_struct)));
            break;
        case value_layout::flat: {
            auto redpanda_field_idx = get_redpanda_idx(
              std::get<iceberg::struct_type>(resolved.type));
            for (size_t i = 0; i < val_struct->fields.size(); ++i) {
                auto& field = val_struct->fields[i];
                if (redpanda_field_idx == i) {
                    rp_struct_value(ret_data).fields.emplace_back(
                      std::move(field));
                    continue;
                }
                ret_data.fields.emplace_back(std::move(field));
            }
            break;
        }
        }
    } else {
        if (parsable_val.has_value()) {
            if (_val_cfg.mode == schema_mode::string) {
                ret_data.fields.emplace_back(
                  std::make_optional<iceberg::value>(iceberg::string_value{
                    utf8_sanitize(std::move(*parsable_val))}));
            } else {
                ret_data.fields.emplace_back(
                  std::make_optional<iceberg::value>(
                    iceberg::binary_value{std::move(*parsable_val)}));
            }
        } else {
            ret_data.fields.emplace_back(std::nullopt);
        }
    }

    co_return ret_data;
}

} // namespace datalake
