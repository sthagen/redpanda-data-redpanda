/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/unicode.h"

#include <cstdlib>
#include <memory>
#include <new>
#include <stdexcept>
#include <utf8proc.h>

namespace iceberg {

namespace {
struct free_deleter {
    void operator()(uint8_t* p) const noexcept {
        // utf8proc_map allocates the output buffer with malloc; we own it.
        // NOLINTNEXTLINE
        free(p);
    }
};

/// Owning wrapper around a utf8proc_map result buffer. The view is valid for
/// the lifetime of this object.
class folded_string {
public:
    folded_string() noexcept = default;
    folded_string(uint8_t* data, size_t size) noexcept
      : owner_(data)
      , size_(size) {}

    std::string_view view() const noexcept {
        return {reinterpret_cast<const char*>(owner_.get()), size_};
    }

private:
    std::unique_ptr<uint8_t, free_deleter> owner_;
    size_t size_ = 0;
};

/// Case-fold \p s using utf8proc. Throws std::bad_alloc on allocation failure
/// or std::runtime_error (with utf8proc_errmsg) on other utf8proc errors such
/// as invalid UTF-8.
folded_string case_fold(std::string_view s) {
    uint8_t* result = nullptr;
    const utf8proc_ssize_t len = utf8proc_map(
      reinterpret_cast<const uint8_t*>(s.data()),
      static_cast<utf8proc_ssize_t>(s.size()),
      &result,
      static_cast<utf8proc_option_t>(UTF8PROC_CASEFOLD | UTF8PROC_COMPOSE));
    if (len == UTF8PROC_ERROR_NOMEM) {
        throw std::bad_alloc{};
    }
    if (len < 0) {
        throw std::runtime_error(utf8proc_errmsg(len));
    }
    return {result, static_cast<size_t>(len)};
}
} // namespace

bool names_equal(
  std::string_view a, std::string_view b, field_name_comparison norm) {
    switch (norm) {
    case field_name_comparison::verbatim:
        return a == b;
    case field_name_comparison::lower_case:
        return case_fold(a).view() == case_fold(b).view();
    }
}

} // namespace iceberg
