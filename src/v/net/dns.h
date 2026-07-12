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
#pragma once
#include "utils/unresolved_address.h"

#include <vector>

namespace net {

/**
 * Resolves addresses using seastar DNS resolver. It uses mutex to workaround
 * seastar bug causing segmentation fault when udp channel is being accessed by
 * different fibers.
 *
 * Returns the first address from resolve_dns_all.
 */
ss::future<ss::socket_address> resolve_dns(const unresolved_address&);

/**
 * Resolves a host name to all of its addresses, each combined with the
 * port of the input address. Never returns an empty list; resolution
 * failures and empty results surface as exceptional futures.
 *
 * The order of the returned addresses carries no preference: for
 * dual-stack names the resolver reports IPv4/IPv6 entries in DNS
 * response arrival order (RFC 6724 sorting is not in effect).
 */
ss::future<std::vector<ss::socket_address>>
resolve_dns_all(const unresolved_address&);

} // namespace net
