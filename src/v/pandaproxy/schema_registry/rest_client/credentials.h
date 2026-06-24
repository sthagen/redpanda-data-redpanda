/*
 * Copyright 2026 Redpanda Data, Inc.
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

#include <seastar/core/sstring.hh>

namespace pandaproxy::schema_registry::rest_client {

/// Credentials for HTTP Basic authentication (RFC 7617) to a Schema Registry.
/// The client holds these as std::optional; an absent value means no
/// Authorization header is sent (an unauthenticated deployment).
struct basic_auth_credentials {
    ss::sstring username;
    ss::sstring password;
};

} // namespace pandaproxy::schema_registry::rest_client
