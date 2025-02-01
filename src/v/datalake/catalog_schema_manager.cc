/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/catalog_schema_manager.h"

#include "base/vlog.h"
#include "datalake/logger.h"
#include "iceberg/compatibility.h"
#include "iceberg/datatypes.h"
#include "iceberg/field_collecting_visitor.h"
#include "iceberg/table_identifier.h"
#include "iceberg/transaction.h"

namespace datalake {

namespace {
schema_manager::errc log_and_convert_catalog_err(
  iceberg::catalog::errc e, std::string_view log_msg) {
    switch (e) {
    case iceberg::catalog::errc::shutting_down:
        vlog(datalake_log.debug, "{}: {}", log_msg, e);
        return schema_manager::errc::shutting_down;
    case iceberg::catalog::errc::timedout:
    case iceberg::catalog::errc::not_found:
    case iceberg::catalog::errc::io_error:
    case iceberg::catalog::errc::unexpected_state:
    case iceberg::catalog::errc::already_exists:
        vlog(datalake_log.warn, "{}: {}", log_msg, e);
        return schema_manager::errc::failed;
    }
}
enum class fill_errc {
    // There is a mismatch in a field's type, name, or required.
    invalid_schema,
    // We couldn't fill all the columns, but the ones we could all matched.
    // Or one or more columns were type-promoted (legally), so we'll need to
    // push an update into the catalog.
    schema_evolution_needed,
};

// Performs a simultaneous, depth-first iteration through fields of the two
// schemas, filling dest's field IDs with those from the source. Returns
// successfully if all the field IDs in the destination type are filled.
checked<std::nullopt_t, fill_errc>
fill_field_ids(iceberg::struct_type& dest, const iceberg::struct_type& source) {
    using namespace iceberg;
    if (auto fill_res = try_fill_field_ids(source, dest);
        fill_res.has_error()) {
        vlog(
          datalake_log.warn,
          "Schema compatibility error: '{}'\n",
          fill_res.error());
        return fill_errc::invalid_schema;
    } else if (!fill_res.value()) {
        // Some fields left without IDs
        return fill_errc::schema_evolution_needed;
    }
    // We successfully filled all the fields in the destination.
    return std::nullopt;
}

// Performs a simultaneous, depth-first iteration through fields of the two
// schemas, filling dest's field IDs with those from the source and checking
// compatibility between the two schemas along the way. Returns successfully if
// the schemas are identical. An error indicates that the schemas differ, in
// which case the errc indicates whether they are compatible. source can be
// correctly evolved to dest.
// NOTE: Post processing is required to assign IDs to any destination fields not
// found in source (i.e. new fields).
checked<std::nullopt_t, fill_errc> check_schema_compat(
  iceberg::struct_type& dest, const iceberg::struct_type& source) {
    using namespace iceberg;
    if (auto evo_res = evolve_schema(source, dest); evo_res.has_error()) {
        vlog(
          datalake_log.warn,
          "Schema compatibility error: '{}'\n",
          evo_res.error());
        return fill_errc::invalid_schema;
    } else if (evo_res.value()) {
        // The schemas are compatible but table metadata needs updating to
        // reflect dest
        return fill_errc::schema_evolution_needed;
    }
    // The schemas are identical
    return std::nullopt;
}
} // namespace

std::ostream& operator<<(std::ostream& o, const schema_manager::errc& e) {
    switch (e) {
    case schema_manager::errc::not_supported:
        return o << "schema_manager::errc::not_supported";
    case schema_manager::errc::failed:
        return o << "schema_manager::errc::failed";
    case schema_manager::errc::shutting_down:
        return o << "schema_manager::errc::shutting_down";
    }
}

bool schema_manager::table_info::fill_registered_ids(
  iceberg::struct_type& type) {
    auto fill_res = fill_field_ids(type, schema.schema_struct);
    return !fill_res.has_error();
}

ss::future<checked<std::nullopt_t, schema_manager::errc>>
simple_schema_manager::ensure_table_schema(
  const iceberg::table_identifier& table_id,
  const iceberg::struct_type& desired_type,
  const iceberg::unresolved_partition_spec& partition_spec) {
    iceberg::schema s{
      .schema_struct = desired_type.copy(),
      .schema_id = {},
      .identifier_field_ids = {},
    };
    if (auto schm_res = s.assign_fresh_ids(); schm_res.has_error()) {
        co_return errc::failed;
    }

    auto resolve_res = iceberg::partition_spec::resolve(
      partition_spec, s.schema_struct);
    if (!resolve_res) {
        co_return errc::failed;
    }

    // TODO: check schema compatibility
    table_info_by_id.insert_or_assign(
      table_id.copy(),
      table_info{
        .id = table_id.copy(),
        .schema = std::move(s),
        .partition_spec = std::move(resolve_res.value()),
        .location = iceberg::uri(fmt::format(
          "{}/{}",
          table_location_prefix_(),
          fmt::join(table_id.ns, "/"),
          table_id.table)),
      });

    co_return std::nullopt;
}

ss::future<checked<schema_manager::table_info, schema_manager::errc>>
simple_schema_manager::get_table_info(
  const iceberg::table_identifier& table_id) {
    auto it = table_info_by_id.find(table_id);
    if (it == table_info_by_id.end()) {
        co_return errc::failed;
    }
    co_return table_info{
      .id = it->second.id.copy(),
      .schema = it->second.schema.copy(),
      .partition_spec = it->second.partition_spec.copy(),
      .location = it->second.location,
    };
}

ss::future<checked<std::nullopt_t, schema_manager::errc>>
catalog_schema_manager::ensure_table_schema(
  const iceberg::table_identifier& table_id,
  const iceberg::struct_type& desired_type,
  const iceberg::unresolved_partition_spec& partition_spec) {
    auto load_res = co_await catalog_.load_or_create_table(
      table_id, desired_type, partition_spec);
    if (load_res.has_error()) {
        co_return log_and_convert_catalog_err(
          load_res.error(), fmt::format("Error loading table {}", table_id));
    }

    // Check schema compatibility
    auto type_copy = desired_type.copy();
    auto get_res = get_ids_from_table_meta(
      table_id, load_res.value(), type_copy);
    if (get_res.has_error()) {
        co_return get_res.error();
    }
    if (get_res.value()) {
        // Success! Schema already matches what we need.
        co_return std::nullopt;
    }

    // The desired schema is backwards compatible with the current table schema.
    // Add the schema to the table.
    iceberg::transaction txn(std::move(load_res.value()));
    auto update_res = co_await txn.set_schema(iceberg::schema{
      .schema_struct = std::move(type_copy),
      .schema_id = iceberg::schema::unassigned_id,
      .identifier_field_ids = {},
    });
    if (update_res.has_error()) {
        auto msg = fmt::format(
          "Failed trying to apply schema update to table {}: {}",
          table_id,
          update_res.error());
        switch (update_res.error()) {
        case iceberg::action::errc::shutting_down:
            vlog(datalake_log.debug, "{}", msg);
            co_return errc::shutting_down;
        case iceberg::action::errc::io_failed:
        case iceberg::action::errc::unexpected_state:
            vlog(datalake_log.warn, "{}", msg);
            co_return errc::failed;
        }
    }
    auto commit_res = co_await catalog_.commit_txn(table_id, std::move(txn));
    if (commit_res.has_error()) {
        co_return log_and_convert_catalog_err(
          commit_res.error(),
          fmt::format(
            "Error while committing schema update to table {}", table_id));
    }
    co_return std::nullopt;
}

ss::future<checked<schema_manager::table_info, schema_manager::errc>>
catalog_schema_manager::get_table_info(
  const iceberg::table_identifier& table_id) {
    auto load_res = co_await catalog_.load_table(table_id);
    if (load_res.has_error()) {
        co_return log_and_convert_catalog_err(
          load_res.error(),
          fmt::format(
            "Error while reloading table {} after schema update", table_id));
    }
    const auto& table = load_res.value();

    auto cur_schema = table.get_schema(table.current_schema_id);
    if (!cur_schema) {
        vlog(
          datalake_log.error,
          "Cannot find current schema {} in table {}",
          table.current_schema_id,
          table_id);
        co_return errc::failed;
    }

    auto cur_spec = table.get_partition_spec(table.default_spec_id);
    if (!cur_spec) {
        vlog(
          datalake_log.error,
          "Cannot find default partition spec {} in table {}",
          table.default_spec_id,
          table_id);
        co_return errc::failed;
    }

    co_return table_info{
      .id = table_id.copy(),
      .schema = cur_schema->copy(),
      .partition_spec = cur_spec->copy(),
      .location = table.location,
    };
}

checked<bool, schema_manager::errc>
catalog_schema_manager::get_ids_from_table_meta(
  const iceberg::table_identifier& table_id,
  const iceberg::table_metadata& table_meta,
  iceberg::struct_type& dest_type) {
    auto schema_iter = std::ranges::find(
      table_meta.schemas,
      table_meta.current_schema_id,
      &iceberg::schema::schema_id);
    if (schema_iter == table_meta.schemas.end()) {
        vlog(
          datalake_log.error,
          "Cannot find current schema {} in table {}",
          table_meta.current_schema_id,
          table_id);
        return errc::failed;
    }
    auto compat_res = check_schema_compat(
      dest_type, schema_iter->schema_struct);
    if (compat_res.has_error()) {
        switch (compat_res.error()) {
        case fill_errc::invalid_schema:
            vlog(datalake_log.warn, "Type mismatch with table {}", table_id);
            return errc::not_supported;
        case fill_errc::schema_evolution_needed:
            return false;
        }
    }
    return true;
}

} // namespace datalake
