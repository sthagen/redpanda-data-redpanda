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
#include "utils/unresolved_address.h"

#include "absl/strings/numbers.h"
#include "base/vlog.h"

#include <ranges>

namespace net {
unresolved_address
unresolved_address::from_string(std::string_view maybe_address) {
    auto split = maybe_address | std::views::split(':')
                 | std::views::transform(
                   [](auto&& subrange) { return std::string_view(subrange); })
                 | std::ranges::to<std::vector<std::string_view>>();

    if (split.size() != 2) {
        throw std::invalid_argument(fmt_with_ctx(
          fmt::format,
          "expected address format: 'host:port', found: {}",
          maybe_address));
    }

    auto host_view = split[0];
    if (host_view.empty()) {
        throw std::invalid_argument(fmt_with_ctx(
          fmt::format, "host is empty in address: {}", maybe_address));
    }
    auto port_view = split[1];

    uint16_t port = 0;
    int32_t port_from_uri{0};
    auto result = absl::SimpleAtoi(port_view, &port_from_uri);
    if (
      !result || port_from_uri < 0
      || port_from_uri > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument(fmt_with_ctx(
          fmt::format, "failed to convert {} to port (uint16_t)", port_view));
    }
    port = static_cast<uint16_t>(port_from_uri);

    return net::unresolved_address{{host_view.data(), host_view.size()}, port};
}
} // namespace net
