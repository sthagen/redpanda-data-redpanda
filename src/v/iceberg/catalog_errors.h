/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <ostream>
#include <string_view>

namespace iceberg {

enum class catalog_errc : uint8_t {
    // There was a problem at the IO layer.
    io_error,

    // IO has timed out. Depending on the caller, may be worth retrying.
    timedout,

    // There was some unexpected state (e.g. a broken invariant in the loaded
    // metadata).
    unexpected_state,

    // There was a problem that indicates the system is shutting down. Best to
    // quiesce operation.
    shutting_down,

    // E.g. a given table already exists.
    already_exists,

    // E.g. a table is not found.
    not_found,
};

constexpr std::string_view to_string_view(catalog_errc e) {
    switch (e) {
    case catalog_errc::io_error:
        return "io_error";
    case catalog_errc::timedout:
        return "timedout";
    case catalog_errc::unexpected_state:
        return "unexpected_state";
    case catalog_errc::shutting_down:
        return "shutting_down";
    case catalog_errc::already_exists:
        return "already_exists";
    case catalog_errc::not_found:
        return "not_found";
    }
}

inline std::ostream& operator<<(std::ostream& o, catalog_errc e) {
    return o << to_string_view(e);
}

/// Rich error for catalog describe, carrying a human-readable message
/// suitable for surfacing to the user via the admin API.
struct catalog_describe_error {
    catalog_errc errc;
    ss::sstring message;
};

} // namespace iceberg
