/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "net/dns.h"

#include "utils/unresolved_address.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

SEASTAR_THREAD_TEST_CASE(dns_resolve) {
    net::unresolved_address ipv4_addr("127.0.0.1", 19092);
    auto ipv4 = net::resolve_dns(ipv4_addr).get();

    BOOST_REQUIRE_EQUAL(ipv4.port(), ipv4_addr.port());
    BOOST_REQUIRE_EQUAL(
      ipv4.family(), static_cast<short>(ss::net::inet_address::family::INET));
    BOOST_REQUIRE(ipv4.addr().is_ipv4());
    BOOST_REQUIRE_EQUAL(ipv4.addr().hostname().get(), "localhost");

    net::unresolved_address ipv6_addr("::1", 19093);
    auto ipv6 = net::resolve_dns(ipv6_addr).get();
    BOOST_REQUIRE_EQUAL(ipv6.port(), ipv6_addr.port());
    BOOST_REQUIRE_EQUAL(
      ipv6.family(), static_cast<short>(ss::net::inet_address::family::INET6));
    BOOST_REQUIRE(ipv6.addr().is_ipv6());
}

SEASTAR_THREAD_TEST_CASE(dns_resolve_all) {
    net::unresolved_address ipv4_addr("127.0.0.1", 19092);
    auto ipv4 = net::resolve_dns_all(ipv4_addr).get();
    BOOST_REQUIRE_EQUAL(ipv4.size(), 1);
    BOOST_REQUIRE(ipv4.front().addr().is_ipv4());
    BOOST_REQUIRE_EQUAL(ipv4.front().port(), ipv4_addr.port());

    net::unresolved_address ipv6_addr("::1", 19093);
    auto ipv6 = net::resolve_dns_all(ipv6_addr).get();
    BOOST_REQUIRE_EQUAL(ipv6.size(), 1);
    BOOST_REQUIRE(ipv6.front().addr().is_ipv6());
    BOOST_REQUIRE_EQUAL(ipv6.front().port(), ipv6_addr.port());

    net::unresolved_address localhost_addr("localhost", 19094);
    auto all = net::resolve_dns_all(localhost_addr).get();
    BOOST_REQUIRE(!all.empty());
    for (const auto& address : all) {
        BOOST_REQUIRE_EQUAL(address.port(), localhost_addr.port());
    }
}
