/*
 * Copyright 2024 Redpanda Data, Inc.
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

#include <seastar/util/bool_class.hh>

#include <string_view>

namespace crypto {
enum class digest_type { MD5, SHA256, SHA512 };

inline fmt::iterator format_to(digest_type t, fmt::iterator out) {
    switch (t) {
    case digest_type::MD5:
        return fmt::format_to(out, "MD5");
    case digest_type::SHA256:
        return fmt::format_to(out, "SHA256");
    case digest_type::SHA512:
        return fmt::format_to(out, "SHA512");
    }
    return fmt::format_to(out, "unknown_digest_type");
}

enum class key_type { RSA };

inline fmt::iterator format_to(key_type t, fmt::iterator out) {
    switch (t) {
    case key_type::RSA:
        return fmt::format_to(out, "RSA");
    }
    return fmt::format_to(out, "unknown_key_type");
}

enum class format_type { PEM, DER };

inline fmt::iterator format_to(format_type t, fmt::iterator out) {
    switch (t) {
    case format_type::PEM:
        return fmt::format_to(out, "PEM");
    case format_type::DER:
        return fmt::format_to(out, "DER");
    }
    return fmt::format_to(out, "unknown_format_type");
}

using is_private_key_t = ss::bool_class<struct is_private_key_tag>;
} // namespace crypto
