/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "absl/numeric/int128.h"
#include "base/format_to.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "iceberg/datatypes.h"
#include "utils/uuid.h"

#include <optional>
#include <variant>

template<>
struct fmt::formatter<absl::int128> : fmt::ostream_formatter {};

template<>
struct fmt::formatter<absl::uint128> : fmt::ostream_formatter {};

namespace iceberg {

struct boolean_value {
    static std::string_view name() { return "boolean"; }
    bool val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "boolean({})", val);
    }
};

struct int_value {
    static std::string_view name() { return "int"; }
    int32_t val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "int({})", val);
    }
};

struct long_value {
    static std::string_view name() { return "long"; }
    int64_t val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "long({})", val);
    }
};

struct float_value {
    static std::string_view name() { return "float"; }
    float val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "float({})", val);
    }
};

struct double_value {
    static std::string_view name() { return "double"; }
    double val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "double({})", val);
    }
};

struct date_value {
    static std::string_view name() { return "date"; }
    // Days since 1970-01-01.
    int32_t val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "date({})", val);
    }
};

struct time_value {
    static std::string_view name() { return "time"; }
    // Microseconds since midnight.
    int64_t val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "time({})", val);
    }
};

struct timestamp_value {
    static std::string_view name() { return "timestamp"; }
    // Microseconds since 1970-01-01 00:00:00.
    int64_t val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "timestamp({})", val);
    }
};

struct timestamptz_value {
    static std::string_view name() { return "timestamptz"; }
    // Microseconds since 1970-01-01 00:00:00 UTC.
    int64_t val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "timestamptz({})", val);
    }
};

struct string_value {
    static std::string_view name() { return "string"; }
    iobuf val;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct uuid_value {
    static std::string_view name() { return "uuid"; }
    uuid_t val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "uuid({})", ss::sstring(val));
    }
};

struct fixed_value {
    static std::string_view name() { return "fixed"; }
    iobuf val;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct binary_value {
    static std::string_view name() { return "binary"; }
    iobuf val;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct decimal_value {
    static std::string_view name() { return "decimal"; }
    absl::int128 val;
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "decimal({})", val);
    }
};

using primitive_value = std::variant<
  boolean_value,
  int_value,
  long_value,
  float_value,
  double_value,
  date_value,
  time_value,
  timestamp_value,
  timestamptz_value,
  string_value,
  uuid_value,
  fixed_value,
  binary_value,
  decimal_value>;
bool operator==(const primitive_value&, const primitive_value&);
bool operator<(const primitive_value&, const primitive_value&);
primitive_value make_copy(const primitive_value&);

struct struct_value;
struct list_value;
struct map_value;
using value = std::variant<
  primitive_value,
  std::unique_ptr<struct_value>,
  std::unique_ptr<list_value>,
  std::unique_ptr<map_value>>;

struct struct_value {
    // The order of these fields must align with the corresponding struct type
    // as defined in the schema, see `iceberg::struct_type`.
    chunked_vector<std::optional<value>> fields;
    fmt::iterator format_to(fmt::iterator it) const;
};
bool operator==(const struct_value&, const struct_value&);
bool operator==(
  const std::unique_ptr<struct_value>&, const std::unique_ptr<struct_value>&);

struct list_value {
    chunked_vector<std::optional<value>> elements;
    fmt::iterator format_to(fmt::iterator it) const;
};
bool operator==(const list_value&, const list_value&);
bool operator==(
  const std::unique_ptr<struct_value>&, const std::unique_ptr<struct_value>&);

struct kv_value {
    // Shouldn't be null, according to the Iceberg spec.
    value key;

    // May be null if the value is null.
    std::optional<value> val;
};
bool operator==(const kv_value&, const kv_value&);

struct map_value {
    chunked_vector<kv_value> kvs;
    fmt::iterator format_to(fmt::iterator it) const;
};
bool operator==(const map_value&, const map_value&);
bool operator==(
  const std::unique_ptr<map_value>&, const std::unique_ptr<map_value>&);
bool operator==(const value&, const value&);

value make_copy(const value&);

size_t value_hash(const struct_value&);
size_t value_hash(const value&);

// Provides the mapping between the c++ type of a primitive iceberg value
// variant and the c++ type of the corresponding primitive iceberg type variant.
template<typename TVal>
struct primitive_value_type {};
template<>
struct primitive_value_type<boolean_value> {
    using type = boolean_type;
};
template<>
struct primitive_value_type<int_value> {
    using type = int_type;
};
template<>
struct primitive_value_type<long_value> {
    using type = long_type;
};
template<>
struct primitive_value_type<float_value> {
    using type = float_type;
};
template<>
struct primitive_value_type<double_value> {
    using type = double_type;
};
template<>
struct primitive_value_type<decimal_value> {
    using type = decimal_type;
};
template<>
struct primitive_value_type<date_value> {
    using type = date_type;
};
template<>
struct primitive_value_type<time_value> {
    using type = time_type;
};
template<>
struct primitive_value_type<timestamp_value> {
    using type = timestamp_type;
};
template<>
struct primitive_value_type<timestamptz_value> {
    using type = timestamptz_type;
};
template<>
struct primitive_value_type<string_value> {
    using type = string_type;
};
template<>
struct primitive_value_type<uuid_value> {
    using type = uuid_type;
};
template<>
struct primitive_value_type<fixed_value> {
    using type = fixed_type;
};
template<>
struct primitive_value_type<binary_value> {
    using type = binary_type;
};

template<typename TVal>
using primitive_value_type_t = typename primitive_value_type<TVal>::type;

fmt::iterator format_to(const primitive_value& v, fmt::iterator it);
fmt::iterator format_to(const value& v, fmt::iterator it);

} // namespace iceberg

template<>
struct fmt::formatter<iceberg::primitive_value> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }
    fmt::iterator
    format(const iceberg::primitive_value& v, fmt::format_context& ctx) const {
        return iceberg::format_to(v, ctx.out());
    }
};

template<>
struct fmt::formatter<iceberg::value> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }
    fmt::iterator
    format(const iceberg::value& v, fmt::format_context& ctx) const {
        return iceberg::format_to(v, ctx.out());
    }
};

namespace std {

template<>
struct hash<iceberg::struct_value> {
    size_t operator()(const iceberg::struct_value& v) const {
        return iceberg::value_hash(v);
    }
};

template<>
struct hash<iceberg::value> {
    size_t operator()(const iceberg::value& v) const {
        return iceberg::value_hash(v);
    }
};

} // namespace std
