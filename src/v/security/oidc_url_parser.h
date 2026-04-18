/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"
#include "base/outcome.h"
#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

namespace security::oidc {

struct parsed_url {
    ss::sstring scheme;
    ss::sstring host;
    uint16_t port;
    ss::sstring target;
    friend bool operator==(const parsed_url&, const parsed_url&) = default;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}://{}:{}{}", scheme, host, port, target);
    }
};
result<parsed_url> parse_url(std::string_view);

} // namespace security::oidc
