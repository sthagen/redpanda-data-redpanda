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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/schema_identifier.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace datalake {

struct record_type {
    record_schema_components comps;
    iceberg::struct_type type;
};

/// Translates Kafka records into Iceberg rows. Key, value, and header handling
/// are each driven by the section configs baked in at construction time:
///
/// - key_cfg.mode binary:  key stored as a raw binary blob in the redpanda
///                         system struct.
/// - key_cfg.mode schema:  key is schema-decoded; the key field in the
///                         redpanda system struct is replaced with the schema
///                         type.
/// - val_cfg.mode binary:  value stored as a raw binary "value" blob column.
/// - val_cfg.mode schema:  value is schema-decoded and its fields are promoted
///                         to top-level columns.
///
/// The resolved types (key_type, val_type) passed at call time are the runtime
/// fulfillment of the declared config. Mismatches are treated as errors.
class record_translator {
public:
    enum class errc {
        translation_error,
        unexpected_schema,
    };
    friend fmt::iterator format_to(errc e, fmt::iterator out) {
        switch (e) {
        case errc::translation_error:
            return fmt::format_to(
              out, "record_translator::errc::translation_error");
        case errc::unexpected_schema:
            return fmt::format_to(
              out, "record_translator::errc::unexpected_schema");
        }
    }

    explicit record_translator(
      model::iceberg_mode::key_config key_cfg = {},
      model::iceberg_mode::value_config val_cfg = {},
      model::iceberg_mode::headers_config headers_cfg = {})
      : _key_cfg(key_cfg)
      , _val_cfg(val_cfg)
      , _headers_cfg(headers_cfg) {}

    record_type build_type(
      std::optional<shared_resolved_type_t> key_type,
      std::optional<shared_resolved_type_t> val_type);

    ss::future<checked<iceberg::struct_value, errc>> translate_data(
      model::partition_id pid,
      kafka::offset o,
      std::optional<shared_resolved_type_t> key_type,
      std::optional<iobuf> parsable_key,
      std::optional<shared_resolved_type_t> val_type,
      std::optional<iobuf> parsable_val,
      model::timestamp ts,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>& headers);

private:
    model::iceberg_mode::key_config _key_cfg;
    model::iceberg_mode::value_config _val_cfg;
    model::iceberg_mode::headers_config _headers_cfg;
};

} // namespace datalake
