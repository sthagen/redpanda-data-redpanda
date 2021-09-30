/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/avro.h"

#include "pandaproxy/schema_registry/error.h"
#include "utils/string_switch.h"

#include <avro/Compiler.hh>
#include <avro/Exception.hh>
#include <avro/GenericDatum.hh>
#include <avro/Types.hh>
#include <avro/ValidSchema.hh>
#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string_view>

namespace pandaproxy::schema_registry {

namespace {

bool check_compatible(avro::Node& reader, avro::Node& writer) {
    if (reader.type() == writer.type()) {
        // Do a quick check first
        if (!writer.resolve(reader)) {
            return false;
        }
        if (reader.type() == avro::Type::AVRO_RECORD) {
            // Recursively check fields
            for (size_t r_idx = 0; r_idx < reader.names(); ++r_idx) {
                size_t w_idx{0};
                if (writer.nameIndex(reader.nameAt(int(r_idx)), w_idx)) {
                    // schemas for fields with the same name in both records are
                    // resolved recursively.
                    if (!check_compatible(
                          *reader.leafAt(int(r_idx)),
                          *writer.leafAt(int(w_idx)))) {
                        return false;
                    }
                } else if (
                  reader.defaultValueAt(int(r_idx)).type() == avro::AVRO_NULL) {
                    // if the reader's record schema has a field with no default
                    // value, and writer's schema does not have a field with the
                    // same name, an error is signalled.
                    return false;
                }
            }
            return true;
        } else if (reader.type() == avro::AVRO_ENUM) {
            // if the writer's symbol is not present in the reader's enum and
            // the reader has a default value, then that value is used,
            // otherwise an error is signalled.
            if (reader.defaultValueAt(0).type() != avro::AVRO_NULL) {
                return true;
            }
            for (size_t w_idx = 0; w_idx < writer.names(); ++w_idx) {
                size_t r_idx{0};
                if (!reader.nameIndex(writer.nameAt(int(w_idx)), r_idx)) {
                    return false;
                }
            }
        } else if (reader.type() == avro::AVRO_UNION) {
            // The first schema in the reader's union that matches the selected
            // writer's union schema is recursively resolved against it. if none
            // match, an error is signalled.
            //
            // Alternatively, any reader must match every writer schema
            for (size_t w_idx = 0; w_idx < writer.leaves(); ++w_idx) {
                bool is_compat = false;
                for (size_t r_idx = 0; r_idx < reader.leaves(); ++r_idx) {
                    if (check_compatible(
                          *reader.leafAt(int(r_idx)),
                          *writer.leafAt(int(w_idx)))) {
                        is_compat = true;
                    }
                }
                if (!is_compat) {
                    return false;
                }
            }
            return true;
        }
    } else if (reader.type() == avro::AVRO_UNION) {
        // The first schema in the reader's union that matches the writer's
        // schema is recursively resolved against it. If none match, an error is
        // signalled.
        //
        // Alternatively, any schema in the reader union must match writer.
        for (size_t r_idx = 0; r_idx < reader.leaves(); ++r_idx) {
            if (check_compatible(*reader.leafAt(int(r_idx)), writer)) {
                return true;
            }
        }
        return false;
    } else if (writer.type() == avro::AVRO_UNION) {
        // If the reader's schema matches the selected writer's schema, it is
        // recursively resolved against it. If they do not match, an error is
        // signalled.
        //
        // Alternatively, reader must match all schema in writer union.
        for (size_t w_idx = 0; w_idx < writer.leaves(); ++w_idx) {
            if (!check_compatible(reader, *writer.leafAt(int(w_idx)))) {
                return false;
            }
        }
        return true;
    }
    return writer.resolve(reader) != avro::RESOLVE_NO_MATCH;
}

result<void> sanitize(
  rapidjson::GenericValue<rapidjson::UTF8<>>& v,
  rapidjson::MemoryPoolAllocator<>& alloc);
result<void> sanitize(
  rapidjson::GenericValue<rapidjson::UTF8<>>::Object& o,
  rapidjson::MemoryPoolAllocator<>& alloc);
result<void> sanitize(
  rapidjson::GenericValue<rapidjson::UTF8<>>::Array& a,
  rapidjson::MemoryPoolAllocator<>& alloc);

result<void> sanitize_union_symbol_name(
  rapidjson::GenericValue<rapidjson::UTF8<>>& name,
  rapidjson::MemoryPoolAllocator<>& alloc) {
    // A name should have the leading dot stripped iff it's the only one

    if (!name.IsString() || name.GetStringLength() == 0) {
        return error_info{
          error_code::schema_invalid, "Invalid JSON Field \"name\""};
    }

    std::string_view fullname_sv{name.GetString(), name.GetStringLength()};
    auto last_dot = fullname_sv.find_last_of('.');

    if (last_dot == 0) {
        fullname_sv.remove_prefix(1);
        // SetString uses memcpy, take a copy so the range doesn't overlap.
        auto new_name = ss::sstring{fullname_sv};
        name.SetString(new_name.data(), new_name.length(), alloc);
    }
    return outcome::success();
}

result<void> sanitize_record(
  rapidjson::GenericValue<rapidjson::UTF8<>>::Object& v,
  rapidjson::MemoryPoolAllocator<>& alloc) {
    auto f_it = v.FindMember("fields");
    if (f_it == v.MemberEnd()) {
        return error_info{
          error_code::schema_invalid, "Missing JSON field \"fields\""};
    }
    if (!f_it->value.IsArray()) {
        return error_info{
          error_code::schema_invalid, "JSON field \"fields\" is not an array"};
    }
    return sanitize(f_it->value, alloc);
}

result<void> sanitize_avro_type(
  rapidjson::GenericValue<rapidjson::UTF8<>>::Object& o,
  std::string_view type_sv,
  rapidjson::MemoryPoolAllocator<>& alloc) {
    auto type = string_switch<std::optional<avro::Type>>(type_sv)
                  .match("record", avro::Type::AVRO_RECORD)
                  .default_match(std::nullopt);
    if (!type.has_value()) {
        return outcome::success();
    }

    switch (type.value()) {
    case avro::AVRO_RECORD: {
        return sanitize_record(o, alloc);
    }
    default:
        break;
    }
    return outcome::success();
}

result<void> sanitize(
  rapidjson::GenericValue<rapidjson::UTF8<>>& v,
  rapidjson::MemoryPoolAllocator<>& alloc) {
    switch (v.GetType()) {
    case rapidjson::Type::kObjectType: {
        auto o = v.GetObject();
        return sanitize(o, alloc);
    }
    case rapidjson::Type::kArrayType: {
        auto a = v.GetArray();
        return sanitize(a, alloc);
    }
    case rapidjson::Type::kFalseType:
    case rapidjson::Type::kTrueType:
    case rapidjson::Type::kNullType:
    case rapidjson::Type::kNumberType:
    case rapidjson::Type::kStringType:
        return outcome::success();
    }
    __builtin_unreachable();
}

result<void> sanitize(
  rapidjson::GenericValue<rapidjson::UTF8<>>::Object& o,
  rapidjson::MemoryPoolAllocator<>& alloc) {
    if (auto it = o.FindMember("name"); it != o.MemberEnd()) {
        // A name should have the leading dot stripped iff it's the only one
        // Otherwise split on the last dot into a name and a namespace

        auto& name = it->value;

        if (!name.IsString() || name.GetStringLength() == 0) {
            return error_info{
              error_code::schema_invalid, "Invalid JSON Field \"name\""};
        }

        std::string_view fullname_sv{name.GetString(), name.GetStringLength()};
        auto last_dot = fullname_sv.find_last_of('.');

        ss::sstring new_namespace;
        if (last_dot != std::string::npos) {
            // Take a copy, fullname_sv will be invalidated when new_name is
            // set, and SetString uses memcpy, the range musn't overlap.
            ss::sstring fullname{fullname_sv};
            fullname_sv = fullname;

            auto new_name{fullname_sv.substr(last_dot + 1)};
            name.SetString(new_name.data(), new_name.length(), alloc);

            fullname.resize(last_dot);
            new_namespace = std::move(fullname);
        }

        if (!new_namespace.empty()) {
            if (auto it = o.FindMember("namespace"); it != o.MemberEnd()) {
                if (!it->value.IsString()) {
                    return error_info{
                      error_code::schema_invalid,
                      "Invalid JSON Field \"namespace\""};
                }
                std::string_view existing_namespace{
                  it->value.GetString(), it->value.GetStringLength()};
                if (existing_namespace != new_namespace) {
                    return error_info{
                      error_code::schema_invalid,
                      "name doesn't match namespace"};
                }
            } else {
                o.AddMember(
                  rapidjson::Value("namespace", alloc),
                  rapidjson::Value(
                    new_namespace.data(), new_namespace.length(), alloc),
                  alloc);
            }
        }
    }

    if (auto t_it = o.FindMember("type"); t_it != o.MemberEnd()) {
        auto res = sanitize(t_it->value, alloc);
        if (res.has_error()) {
            return res.assume_error();
        }

        if (t_it->value.GetType() == rapidjson::Type::kStringType) {
            std::string_view type_sv = {
              t_it->value.GetString(), t_it->value.GetStringLength()};
            auto res = sanitize_avro_type(o, type_sv, alloc);
            if (res.has_error()) {
                return res.assume_error();
            }
        }
        if (t_it->value.GetType() == rapidjson::Type::kArrayType) {
            auto a = t_it->value.GetArray();
            for (auto& m : a) {
                if (m.IsString()) {
                    auto res = sanitize_union_symbol_name(m, alloc);
                    if (res.has_error()) {
                        return res.assume_error();
                    }
                }
            }
        }
    }
    return outcome::success();
}

result<void> sanitize(
  rapidjson::GenericValue<rapidjson::UTF8<>>::Array& a,
  rapidjson::MemoryPoolAllocator<>& alloc) {
    for (auto& m : a) {
        auto s = sanitize(m, alloc);
        if (s.has_error()) {
            return s.assume_error();
        }
    }
    return outcome::success();
}

} // namespace

result<avro_schema_definition>
make_avro_schema_definition(std::string_view sv) {
    try {
        return avro_schema_definition{avro::compileJsonSchemaFromMemory(
          reinterpret_cast<const uint8_t*>(sv.data()), sv.length())};
    } catch (const avro::Exception& e) {
        return error_info{
          error_code::schema_invalid,
          fmt::format("Invalid schema {}", e.what())};
    }
}

result<schema_definition>
sanitize_avro_schema_definition(schema_definition def) {
    rapidjson::GenericDocument<rapidjson::UTF8<>> doc;
    constexpr auto flags = rapidjson::kParseDefaultFlags
                           | rapidjson::kParseStopWhenDoneFlag;
    doc.Parse<flags>(def().data(), def().size());
    if (doc.HasParseError()) {
        return error_info{
          error_code::schema_invalid,
          fmt::format(
            "Invalid schema: {} at offset {}",
            rapidjson::GetParseError_En(doc.GetParseError()),
            doc.GetErrorOffset())};
    }

    auto res = sanitize(doc, doc.GetAllocator());
    if (res.has_error()) {
        return error_info{
          res.assume_error().code(),
          fmt::format("{} {}", res.assume_error().message(), def())};
    }

    rapidjson::GenericStringBuffer<rapidjson::UTF8<>> str_buf;
    str_buf.Reserve(def().size());
    rapidjson::Writer<rapidjson::StringBuffer> w{str_buf};

    if (!doc.Accept(w)) {
        return error_info{error_code::schema_invalid, "Invalid schema"};
    }

    return schema_definition{
      ss::sstring{str_buf.GetString(), str_buf.GetSize()}};
}

bool check_compatible(
  const avro_schema_definition& reader, const avro_schema_definition& writer) {
    return check_compatible(*reader().root(), *writer().root());
}

} // namespace pandaproxy::schema_registry
