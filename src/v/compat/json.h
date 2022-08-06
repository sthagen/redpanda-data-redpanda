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

#include "json/document.h"
#include "json/json.h"
#include "model/fundamental.h"
#include "net/unresolved_address.h"
#include "utils/base64.h"

namespace json {

inline char const* to_str(rapidjson::Type const t) {
    static char const* str[] = {
      "Null", "False", "True", "Object", "Array", "String", "Number"};
    return str[t];
}

inline void read_value(json::Value const& v, int64_t& target) {
    target = v.GetInt64();
}

inline void read_value(json::Value const& v, uint64_t& target) {
    target = v.GetUint64();
}

inline void read_value(json::Value const& v, uint32_t& target) {
    target = v.GetUint();
}

inline void read_value(json::Value const& v, int32_t& target) {
    target = v.GetInt();
}

inline void read_value(json::Value const& v, int16_t& target) {
    target = v.GetInt();
}

inline void read_value(json::Value const& v, uint16_t& target) {
    target = v.GetUint();
}

inline void read_value(json::Value const& v, int8_t& target) {
    target = v.GetInt();
}

inline void read_value(json::Value const& v, uint8_t& target) {
    target = v.GetUint();
}

inline void read_value(json::Value const& v, bool& target) {
    target = v.GetBool();
}

inline void read_value(json::Value const& v, ss::sstring& target) {
    target = v.GetString();
}

inline void read_value(json::Value const& v, iobuf& target) {
    target = bytes_to_iobuf(base64_to_bytes(v.GetString()));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::chrono::nanoseconds& v) {
    rjson_serialize(w, v.count());
}

inline void read_value(json::Value const& v, std::chrono::nanoseconds& target) {
    target = std::chrono::nanoseconds(v.GetInt64());
}

template<typename T, typename Tag, typename IsConstexpr>
void read_value(
  json::Value const& v, detail::base_named_type<T, Tag, IsConstexpr>& target) {
    auto t = T{};
    read_value(v, t);
    target = detail::base_named_type<T, Tag, IsConstexpr>{t};
}

inline void
read_value(json::Value const& v, std::chrono::milliseconds& target) {
    target = std::chrono::milliseconds(v.GetUint64());
}

template<typename T>
void read_value(json::Value const& v, std::vector<T>& target) {
    for (auto const& e : v.GetArray()) {
        auto t = T{};
        read_value(e, t);
        target.push_back(t);
    }
}

template<typename T>
void read_value(json::Value const& v, std::optional<T>& target) {
    if (v.IsNull()) {
        target = std::nullopt;
    } else {
        auto t = T{};
        read_value(v, t);
        target = t;
    }
}

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const iobuf& buf) {
    w.String(bytes_to_base64(iobuf_to_bytes(buf)));
}

template<typename Writer, typename T>
void write_member(Writer& w, char const* key, T const& value) {
    w.String(key);
    rjson_serialize(w, value);
}

template<typename T>
void read_member(json::Value const& v, char const* key, T& target) {
    auto const it = v.FindMember(key);
    if (it != v.MemberEnd()) {
        read_value(it->value, target);
    } else {
        target = {};
        std::cout << "key " << key << " not found, default initializing"
                  << std::endl;
    }
}

template<typename Enum>
inline auto read_member_enum(json::Value const& v, char const* key, Enum)
  -> std::underlying_type_t<Enum> {
    std::underlying_type_t<Enum> value;
    read_member(v, key, value);
    return value;
}

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const model::ntp& ntp) {
    w.StartObject();
    w.Key("ns");
    w.String(ntp.ns());
    w.Key("topic");
    w.String(ntp.tp.topic());
    w.Key("partition");
    w.Int(ntp.tp.partition());
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::ntp& obj) {
    read_member(rd, "ns", obj.ns);
    read_member(rd, "topic", obj.tp.topic);
    read_member(rd, "partition", obj.tp.partition);
}

template<typename T>
void read_value(json::Value const& v, ss::bool_class<T>& target) {
    target = ss::bool_class<T>(v.GetBool());
}

template<typename T, typename V>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::unordered_map<T, V>& m) {
    w.StartArray();
    for (const auto& e : m) {
        w.StartObject();
        w.Key("key");
        rjson_serialize(w, e.first);
        w.Key("value");
        rjson_serialize(w, e.second);
        w.EndObject();
    }
    w.EndArray();
}

template<typename T, typename V>
inline void read_value(json::Value const& rd, std::unordered_map<T, V>& obj) {
    for (const auto& e : rd.GetArray()) {
        T key;
        read_member(e, "key", key);
        V value;
        read_member(e, "value", value);
        obj.emplace(std::move(key), std::move(value));
    }
}

template<typename T>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const ss::bool_class<T>& b) {
    rjson_serialize(w, bool(b));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::broker_properties& b) {
    w.StartObject();
    w.Key("cores");
    rjson_serialize(w, b.cores);
    w.Key("available_memory_gb");
    rjson_serialize(w, b.available_memory_gb);
    w.Key("available_disk_gb");
    rjson_serialize(w, b.available_disk_gb);
    w.Key("mount_paths");
    rjson_serialize(w, b.mount_paths);
    w.Key("etc_props");
    rjson_serialize(w, b.etc_props);
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::broker_properties& obj) {
    read_member(rd, "cores", obj.cores);
    read_member(rd, "available_memory_gb", obj.available_memory_gb);
    read_member(rd, "available_disk_gb", obj.available_disk_gb);
    read_member(rd, "mount_paths", obj.mount_paths);
    read_member(rd, "etc_props", obj.etc_props);
}

inline void
read_value(json::Value const& rd, ss::net::inet_address::family& obj) {
    obj = static_cast<ss::net::inet_address::family>(rd.GetInt());
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const ss::net::inet_address::family& b) {
    w.Int(static_cast<int>(b));
}

inline void read_value(json::Value const& rd, net::unresolved_address& obj) {
    ss::sstring host;
    uint16_t port{0};
    read_member(rd, "address", host);
    read_member(rd, "port", port);
    obj = net::unresolved_address(std::move(host), port);
}

inline void read_value(json::Value const& rd, model::broker_endpoint& obj) {
    ss::sstring host;
    uint16_t port{0};

    read_member(rd, "name", obj.name);
    read_member(rd, "address", host);
    read_member(rd, "port", port);

    obj.address = net::unresolved_address(std::move(host), port);
}

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const model::broker& b) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, b.id());
    w.Key("kafka_advertised_listeners");
    rjson_serialize(w, b.kafka_advertised_listeners());
    w.Key("rpc_address");
    rjson_serialize(w, b.rpc_address());
    w.Key("rack");
    rjson_serialize(w, b.rack());
    w.Key("properties");
    rjson_serialize(w, b.properties());
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::broker& obj) {
    model::node_id id;
    std::vector<model::broker_endpoint> kafka_advertised_listeners;
    net::unresolved_address rpc_address;
    std::optional<model::rack_id> rack;
    model::broker_properties properties;

    read_member(rd, "id", id);
    read_member(rd, "kafka_advertised_listeners", kafka_advertised_listeners);
    read_member(rd, "rpc_address", rpc_address);
    read_member(rd, "rack", rack);
    read_member(rd, "properties", properties);

    obj = model::broker(
      id,
      std::move(kafka_advertised_listeners),
      std::move(rpc_address),
      std::move(rack),
      std::move(properties));
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

inline void read_value(json::Value const& rd, model::topic_namespace& obj) {
    model::ns ns;
    model::topic tp;
    read_member(rd, "ns", ns);
    read_member(rd, "tp", tp);
    obj = model::topic_namespace(std::move(ns), std::move(tp));
}

#define json_write(_fname) json::write_member(wr, #_fname, obj._fname)
#define json_read(_fname) json::read_member(rd, #_fname, obj._fname)

} // namespace json
