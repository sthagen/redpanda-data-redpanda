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

#include "base/format_to.h"
#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <forward_list>
#include <utility>

// An error type that includes an error code and a list of detail messages.
// As errors propagate through layers, each layer can wrap the error with
// additional context using wrap().
//
// Example:
//   ```
//   auto inner = error(errc::foo, "inner");
//   auto outer = error::wrap(std::move(inner), errc::bar, "outer");
//   ```
//   Formats as: "[bar]: outer: inner"
template<typename ErrorT>
struct detailed_error {
public:
    explicit detailed_error(ErrorT e)
      : e(std::move(e)) {}
    detailed_error(ErrorT e, ss::sstring detail)
      : e(std::move(e))
      , details({std::move(detail)}) {}

    ~detailed_error() = default;
    detailed_error(const detailed_error&) = delete;
    detailed_error& operator=(const detailed_error&) = delete;
    detailed_error(detailed_error&&) noexcept = default;
    detailed_error& operator=(detailed_error&&) noexcept = default;

    template<typename OtherErrorT>
    detailed_error<OtherErrorT> wrap(OtherErrorT&& other) && {
        detailed_error<OtherErrorT> ret(std::forward<OtherErrorT>(other));
        ret.details = std::move(details);
        return ret;
    }

    template<typename OtherErrorT>
    detailed_error<OtherErrorT>
    wrap(OtherErrorT&& other, ss::sstring detail) && {
        detailed_error<OtherErrorT> ret(std::forward<OtherErrorT>(other));
        ret.details = std::move(details);
        ret.details.push_front(std::move(detail));
        return std::move(ret);
    }

    template<typename OtherErrorT>
    static detailed_error wrap(detailed_error<OtherErrorT> other, ErrorT&& e) {
        return std::move(other).wrap(std::forward<ErrorT>(e));
    }
    template<typename OtherErrorT>
    static detailed_error
    wrap(detailed_error<OtherErrorT> other, ErrorT&& e, ss::sstring detail) {
        return std::move(other).wrap(
          std::forward<ErrorT>(e), std::move(detail));
    }

    fmt::iterator format_to(fmt::iterator it) const {
        it = fmt::format_to(it, "[{}]", e);
        for (const auto& msg : details) {
            it = fmt::format_to(it, ": {}", msg);
        }
        return it;
    }

    ErrorT e;
    std::forward_list<ss::sstring> details;
};
