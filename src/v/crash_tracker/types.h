/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/sstring.h"

#include <ostream>

namespace crash_tracker {

enum class crash_type {
    unknown,
    startup_exception,
    segfault,
    abort,
    illegal_instruction
};

/// reserved_string is a simple wrapper around ss::sstring that allows
/// pre-allocating a string of a certain size with trailing '\0's and allows
/// safer access to the populated part of the string.
struct reserved_string
  : serde::
      envelope<reserved_string, serde::version<0>, serde::compat_version<0>> {
    reserved_string() = default;
    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    reserved_string(ss::sstring other) noexcept
      : _str(std::move(other)) {};

    explicit reserved_string(size_t n)
      : _str(n, '\0') {}

    char* begin() { return _str.begin(); }
    const char* c_str() const noexcept { return _str.c_str(); }

    /// The length of the populated, non-'\0' prefix of the string.
    size_t length() const noexcept { return strlen(_str.c_str()); }

    /// The full capacity of the string, including the all-'\0' suffix.
    size_t capacity() const noexcept { return _str.size(); }

    void serde_write(iobuf& out) { serde::write(out, std::move(_str)); }
    void serde_read(iobuf_parser& in, const serde::header& h) {
        _str = serde::read_nested<ss::sstring>(in, h._bytes_left_limit);
    }

private:
    ss::sstring _str;
};

struct crash_description
  : serde::
      envelope<crash_description, serde::version<0>, serde::compat_version<0>> {
    // We pre-allocate the memory necessary for the buffers needed to fill a
    // crash_description and overestimate its serialized size to be able to
    // pre-allocate a sufficiently large iobuf to write it to
    constexpr static size_t string_buffer_reserve = 4096;
    constexpr static size_t overhead_overestimate = 1024;
    constexpr static size_t serde_size_overestimate
      = overhead_overestimate + 3 * string_buffer_reserve;

    crash_type type{};
    model::timestamp crash_time;
    reserved_string crash_message;
    reserved_string stacktrace;

    /// Extension to the crash_message. It can be used to add further
    /// information about the crash that is useful for debugging but is too
    /// verbose for telemetry.
    /// Eg. top-N allocations
    reserved_string addition_info;

    crash_description()
      : crash_time{}
      , crash_message(string_buffer_reserve)
      , stacktrace(string_buffer_reserve)
      , addition_info(string_buffer_reserve) {}

    auto serde_fields() {
        return std::tie(
          type, crash_time, crash_message, stacktrace, addition_info);
    }

    friend std::ostream& operator<<(std::ostream&, const crash_description&);
};

struct crash_tracker_metadata
  : serde::envelope<
      crash_tracker_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    uint32_t crash_count{0};
    uint64_t config_checksum{0};
    model::timestamp last_start_ts;

    auto serde_fields() {
        return std::tie(crash_count, config_checksum, last_start_ts);
    }
};

class crash_loop_limit_reached : public std::runtime_error {
public:
    explicit crash_loop_limit_reached()
      : std::runtime_error("Crash loop detected, aborting startup.") {}
};

bool is_crash_loop_limit_reached(std::exception_ptr);

} // namespace crash_tracker
