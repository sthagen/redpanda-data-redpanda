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
#include "bytes/iobuf.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/map.h"
#include "serde/rw/optional.h"
#include "utils/to_string.h"

namespace admin::proxy {

struct proxy_request
  : serde::
      envelope<proxy_request, serde::version<0>, serde::compat_version<0>> {
    ss::sstring service; // For example: "redpanda.core.admin.Service"
    ss::sstring method;  // For example: "MyRPCMethod"
    iobuf payload;       // The serialized request protobuf
    // Nodes that have already proxied this request.
    std::vector<model::node_id> via;

    auto serde_fields() { return std::tie(service, method, payload, via); }
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{service={}, method={}, via={}, payload={}}}",
          service,
          method,
          via,
          payload);
    }
};

// ID of valid error codes for the RPC protocol.
//
// See: https://grpc.io/docs/guides/status-codes/
// clang-format off
enum class errc : uint8_t {
  ok = 0,                 // No error; returned on success.
  cancelled = 1,          // The operation was cancelled (typically by the caller).
  unknown = 2,            // Unknown error.
  invalid_argument = 3,   // Client specified an invalid argument.
  deadline_exceeded = 4,  // Deadline expired before operation could complete.
  not_found = 5,          // Some requested entity was not found.
  already_exists = 6,     // Entity that a client attempted to create already exists.
  permission_denied = 7,  // The caller does not have permission to execute the specified operation.
  resource_exhausted = 8, // Some resource has been exhausted.
  failed_precondition = 9,// Operation was rejected because the system is not in a state required for the operation's execution.
  aborted = 10,           // The operation was aborted, typically due to a concurrency issue.
  out_of_range = 11,      // Operation was attempted past the valid range.
  unimplemented = 12,     // Operation is not implemented or not supported/enabled in this service.
  internal_error = 13,    // Internal error occurred in the service.
  unavailable = 14,       // Service is currently unavailable (e.g., due to maintenance).
  data_loss = 15,         // Unrecoverable data loss or corruption.
  unauthenticated = 16,   // The request does not have valid authentication credentials for the operation.
};
// clang-format on

struct proxy_response
  : serde::
      envelope<proxy_response, serde::version<0>, serde::compat_version<0>> {
    errc error_code = errc::ok;
    // If the error_code == errc::ok, this will contain the serialized protobuf
    // response. Otherwise, this will be the error message for the error_code.
    iobuf payload;

    struct error_info
      : serde::
          envelope<error_info, serde::version<0>, serde::compat_version<0>> {
        ss::sstring reason;
        ss::sstring domain;
        chunked_hash_map<ss::sstring, ss::sstring> metadata;
        auto serde_fields() { return std::tie(reason, domain, metadata); }
        fmt::iterator format_to(fmt::iterator it) const {
            return fmt::format_to(
              it,
              "{{reason={}, domain={}, metadata={}}}",
              reason,
              domain,
              metadata);
        }
    };
    std::optional<error_info> info;

    auto serde_fields() { return std::tie(error_code, payload, info); }
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{error_code={}, message={}, error_info={}}}",
          std::to_underlying(error_code),
          payload,
          info);
    }
};

} // namespace admin::proxy
