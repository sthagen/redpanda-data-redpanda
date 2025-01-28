/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "compat/json.h"
#include "model/metadata.h"
#include "model/record.h"

namespace json {

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::timestamp& v) {
    rjson_serialize(wr, v.value());
}

inline void read_value(const json::Value& rd, model::timestamp& out) {
    out = model::timestamp(rd.GetInt64());
}

inline void read_value(const json::Value& rd, model::compression& e) {
    std::underlying_type_t<model::compression> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::compression::none;
        break;
    case 1:
        e = model::compression::gzip;
        break;
    case 2:
        e = model::compression::snappy;
        break;
    case 3:
        e = model::compression::lz4;
        break;
    case 4:
        e = model::compression::zstd;
        break;
    case std::numeric_limits<decltype(value)>::max():
        e = model::compression::producer;
        break;
    default:
        vassert(false, "Unknown enum value model::compression: {}", value);
    }
}

inline void read_value(const json::Value& rd, model::timestamp_type& e) {
    std::underlying_type_t<model::timestamp_type> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::timestamp_type::create_time;
        break;
    case 1:
        e = model::timestamp_type::append_time;
        break;
    default:
        vassert(false, "Unknown enum value model::timestamp_type: {}", value);
    }
}

inline void
read_value(const json::Value& rd, model::cleanup_policy_bitflags& e) {
    std::underlying_type_t<model::cleanup_policy_bitflags> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::cleanup_policy_bitflags::none;
        break;
    case 1U:
        e = model::cleanup_policy_bitflags::deletion;
        break;
    case 1U << 1U:
        e = model::cleanup_policy_bitflags::compaction;
        break;
    default:
        vassert(
          false,
          "Unknown enum value model::cleanup_policy_bitflags: {}",
          value);
    }
}

inline void read_value(const json::Value& rd, model::compaction_strategy& e) {
    std::underlying_type_t<model::compaction_strategy> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::compaction_strategy::offset;
        break;
    case 1:
        e = model::compaction_strategy::timestamp;
        break;
    case 2:
        e = model::compaction_strategy::header;
        break;
    default:
        vassert(
          false, "Unknown enum value model::compaction_strategy: {}", value);
    }
}

inline void read_value(const json::Value& rd, model::shadow_indexing_mode& e) {
    std::underlying_type_t<model::shadow_indexing_mode> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::shadow_indexing_mode::disabled;
        break;
    case 1:
        e = model::shadow_indexing_mode::archival;
        break;
    case 2:
        e = model::shadow_indexing_mode::fetch;
        break;
    case 3:
        e = model::shadow_indexing_mode::full;
        break;
    case 0xfe:
        e = model::shadow_indexing_mode::drop_archival;
        break;

    case 0xfd:
        e = model::shadow_indexing_mode::drop_fetch;
        break;

    case 0xfc:
        e = model::shadow_indexing_mode::drop_full;
        break;
    default:
        vassert(
          false, "Unknown enum value model::shadow_indexing_mode: {}", value);
    }
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::producer_identity& v) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, v.id);
    w.Key("epoch");
    rjson_serialize(w, v.epoch);
    w.EndObject();
}

inline void read_value(const json::Value& rd, model::producer_identity& obj) {
    read_member(rd, "id", obj.id);
    read_member(rd, "epoch", obj.epoch);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::broker_shard& v) {
    w.StartObject();
    w.Key("node_id");
    rjson_serialize(w, v.node_id);
    w.Key("shard");
    rjson_serialize(w, v.shard);
    w.EndObject();
}

inline void read_value(const json::Value& rd, model::broker_shard& obj) {
    read_member(rd, "node_id", obj.node_id);
    read_member(rd, "shard", obj.shard);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::partition_metadata& v) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, v.id);
    w.Key("replicas");
    rjson_serialize(w, v.replicas);
    w.Key("leader_node");
    rjson_serialize(w, v.leader_node);
    w.EndObject();
}

inline void read_value(const json::Value& rd, model::partition_metadata& obj) {
    read_member(rd, "id", obj.id);
    read_member(rd, "replicas", obj.replicas);
    read_member(rd, "leader_node", obj.leader_node);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::topic_namespace& t) {
    w.StartObject();
    w.Key("ns");
    rjson_serialize(w, t.ns);
    w.Key("tp");
    rjson_serialize(w, t.tp);
    w.EndObject();
}

inline void read_value(const json::Value& rd, model::topic_namespace& obj) {
    model::ns ns;
    model::topic tp;
    read_member(rd, "ns", ns);
    read_member(rd, "tp", tp);
    obj = model::topic_namespace(std::move(ns), std::move(tp));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::topic_metadata& tm) {
    w.StartObject();
    write_member(w, "tp_ns", tm.tp_ns);
    write_member(w, "partitions", tm.partitions);
    w.EndObject();
}

inline void read_value(const json::Value& rd, model::topic_metadata& tm) {
    read_member(rd, "tp_ns", tm.tp_ns);
    read_member(rd, "partitions", tm.partitions);
}

// NOTE: These are exceptions to overloads of rjson_serialize(enum) definitions
// defined in v/config/cluster_serializtion.h which print string representations
// of the listed enumerations below. The compat framework expects all enums in
// their json representation to be their respective underlying types. To avoid
// breaking something by modifying the other definitions these types will be
// explicity serialized to json with these special methods

template<typename T>
inline constexpr bool is_exceptional_enum
  = std::is_enum_v<T>
    && (std::is_same_v<T, model::compression> || std::is_same_v<T, model::timestamp_type> || std::is_same_v<T, model::cleanup_policy_bitflags> || std::is_same_v<T, model::write_caching_mode>);

template<typename T>
inline constexpr bool is_exceptional_enum_wrapped_opt
  = is_exceptional_enum<typename T::value_type>;

template<typename T>
void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w, const std::optional<T>& t) {
    if (t) {
        rjson_serialize_exceptional_type(w, *t);
    } else {
        w.Null();
    }
}

template<typename T>
void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w, const tristate<T>& t) {
    w.StartObject();
    w.Key("status");
    if (t.is_disabled()) {
        w.Int(uint8_t(tristate_status::disabled));
    } else if (!t.has_optional_value()) {
        w.Int(uint8_t(tristate_status::not_set));
    } else {
        w.Int(uint8_t(tristate_status::set));
        w.Key("value");
        rjson_serialize_exceptional_type(w, t.value());
    }
    w.EndObject();
}

template<typename Writer, typename T>
void write_exceptional_member_type(Writer& w, const char* key, const T& value) {
    w.String(key);
    rjson_serialize_exceptional_type(w, value);
}

inline void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w, const model::compression& c) {
    using underlying_t = std::underlying_type_t<model::compression>;
    rjson_serialize(w, static_cast<underlying_t>(c));
}

inline void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w, const model::timestamp_type& c) {
    using underlying_t = std::underlying_type_t<model::timestamp_type>;
    rjson_serialize(w, static_cast<underlying_t>(c));
}

inline void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w,
  const model::cleanup_policy_bitflags& c) {
    using underlying_t = std::underlying_type_t<model::cleanup_policy_bitflags>;
    rjson_serialize(w, static_cast<underlying_t>(c));
}

inline void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w, const model::write_caching_mode& m) {
    using underlying_t = std::underlying_type_t<model::write_caching_mode>;
    rjson_serialize(w, static_cast<underlying_t>(m));
}

inline void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w,
  const model::iceberg_invalid_record_action& m) {
    using underlying_t
      = std::underlying_type_t<model::iceberg_invalid_record_action>;
    rjson_serialize(w, static_cast<underlying_t>(m));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr,
  const model::record_batch_attributes& b) {
    rjson_serialize(wr, b.value());
}

inline void
read_value(const json::Value& rd, model::record_batch_attributes& out) {
    out = model::record_batch_attributes(rd.GetInt());
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record_attributes& b) {
    rjson_serialize(wr, b.value());
}

inline void read_value(const json::Value& rd, model::record_attributes& out) {
    out = model::record_attributes(rd.GetInt());
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record_batch_header& obj) {
    wr.StartObject();
    json_write(header_crc);
    json_write(size_bytes);
    json_write(base_offset);
    json_write(type);
    json_write(crc);
    json_write(attrs);
    json_write(last_offset_delta);
    json_write(first_timestamp);
    json_write(max_timestamp);
    json_write(producer_id);
    json_write(producer_epoch);
    json_write(base_sequence);
    json_write(record_count);
    wr.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record_header& obj) {
    wr.StartObject();
    json::write_member(wr, "key_size", obj.key_size());
    json::write_member(wr, "key", obj.key());
    json::write_member(wr, "value_size", obj.value_size());
    json::write_member(wr, "value", obj.value());
    wr.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record& obj) {
    wr.StartObject();
    json::write_member(wr, "size_bytes", obj.size_bytes());
    json::write_member(wr, "attributes", obj.attributes());
    json::write_member(wr, "timestamp_delta", obj.timestamp_delta());
    json::write_member(wr, "offset_delta", obj.offset_delta());
    json::write_member(wr, "key_size", obj.key_size());
    json::write_member(wr, "key", obj.key());
    json::write_member(wr, "value_size", obj.value_size());
    json::write_member(wr, "value", obj.value());
    json::write_member(wr, "headers", obj.headers());
    wr.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record_batch& b) {
    wr.StartObject();
    wr.Key("header");
    rjson_serialize(wr, b.header());
    wr.Key("records");
    rjson_serialize(wr, b.copy_records());
    wr.EndObject();
}

inline void read_value(const json::Value& rd, model::record_batch_header& out) {
    model::record_batch_header obj;
    json_read(header_crc);
    json_read(size_bytes);
    json_read(base_offset);
    json_read(type);
    json_read(crc);
    json_read(attrs);
    json_read(last_offset_delta);
    json_read(first_timestamp);
    json_read(max_timestamp);
    json_read(producer_id);
    json_read(producer_epoch);
    json_read(base_sequence);
    json_read(record_count);
    out = obj;
}

/*
 * specialized circular_buffer for record_batch type because record_batch
 * doesn't support default constructor and the generic form for circular buffer
 * of type T needs to build a structure to deserialize into.
 *
 * since the components of the record batch aren't default constructable we also
 * circumvent the normal api to which requires default ctor is available.
 */
inline void read_value(
  const json::Value& v, ss::circular_buffer<model::record_batch>& target) {
    for (const auto& e : v.GetArray()) {
        model::record_batch_header header;
        std::vector<model::record> records;

        /*
         * header
         */
        json::read_member(e, "header", header);

        /*
         * records
         */
        vassert(
          e.HasMember("records") && e["records"].IsArray(),
          "invalid records field");
        for (const auto& r : e["records"].GetArray()) {
            vassert(r.IsObject(), "record is not an object");

            int32_t size_bytes{};
            model::record_attributes attributes;
            int64_t timestamp_delta{};
            int32_t offset_delta{};
            int32_t key_size{};
            iobuf key;
            int32_t value_size{};
            iobuf value;
            std::vector<model::record_header> headers;

            json::read_member(r, "size_bytes", size_bytes);
            json::read_member(r, "attributes", attributes);
            json::read_member(r, "timestamp_delta", timestamp_delta);
            json::read_member(r, "offset_delta", offset_delta);
            json::read_member(r, "key_size", key_size);
            json::read_member(r, "key", key);
            json::read_member(r, "value_size", value_size);
            json::read_member(r, "value", value);

            /*
             * record headers
             */
            vassert(
              r.HasMember("headers") && r["headers"].IsArray(),
              "invalid headers field");
            for (const auto& h : r["headers"].GetArray()) {
                int32_t key_size{};
                iobuf key;
                int32_t value_size{};
                iobuf value;

                json::read_member(h, "key_size", key_size);
                json::read_member(h, "key", key);
                json::read_member(h, "value_size", value_size);
                json::read_member(h, "value", value);

                headers.emplace_back(
                  key_size, std::move(key), value_size, std::move(value));
            }

            records.emplace_back(
              size_bytes,
              attributes,
              timestamp_delta,
              offset_delta,
              key_size,
              std::move(key),
              value_size,
              std::move(value),
              std::move(headers));
        }

        target.emplace_back(header, std::move(records));
    }
}

} // namespace json
