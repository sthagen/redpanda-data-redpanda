/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "net/dns.h"

#include "ssx/mutex.h"
#include "utils/unresolved_address.h"

#include <seastar/core/coroutine.hh>
#include <seastar/net/dns.hh>

#include <ranges>

namespace net {

namespace {

ss::future<ss::net::hostent> lookup_host(const unresolved_address& address) {
    static thread_local ss::net::dns_resolver resolver;
    static thread_local ssx::mutex m{"resolve_dns"};
    // lock
    auto units = co_await m.get_units();
    // resolve
    auto host = co_await resolver.get_host_by_name(
      address.host(), address.family());
    if (host.addr_entries.empty()) {
        throw std::runtime_error(
          fmt::format(
            "dns resolution of {} returned no addresses", address.host()));
    }
    co_return host;
}

} // namespace

ss::future<ss::socket_address> resolve_dns(const unresolved_address& address) {
    auto host = co_await lookup_host(address);
    co_return ss::socket_address(
      host.addr_entries.front().addr, address.port());
}

ss::future<std::vector<ss::socket_address>>
resolve_dns_all(const unresolved_address& address) {
    auto host = co_await lookup_host(address);

    co_return host.addr_entries
      | std::views::transform([port = address.port()](const auto& entry) {
            return ss::socket_address(entry.addr, port);
        })
      | std::ranges::to<std::vector>();
}

} // namespace net
