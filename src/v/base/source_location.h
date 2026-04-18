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
#include "base/format_to.h"

#include <cstdint>
#include <source_location>

namespace vlog {
namespace detail {
consteval const char* file_basename(
  const char* const path = std::source_location::current().file_name(),
  const int32_t index = 0,
  const int32_t slash_index = -1) {
    // NOLINTNEXTLINE
    const char c = path[index];
    if (c == '\0') {
        // NOLINTNEXTLINE
        return &path[slash_index + 1];
    }
    if (c == '/' || c == '\\') {
        return file_basename(path, index + 1, index);
    }
    return file_basename(path, index + 1, slash_index);
}
} // namespace detail

struct file_line {
    const char* filename;
    unsigned line;

    consteval static file_line
    current(const std::source_location src = std::source_location::current()) {
        return {
          .filename = detail::file_basename(src.file_name()),
          .line = src.line()};
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}:{}", filename, line);
    }
};

} // namespace vlog
