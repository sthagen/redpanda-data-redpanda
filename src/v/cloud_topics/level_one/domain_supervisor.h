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

#include "base/seastarx.h"

#include <seastar/core/future.hh>

namespace cluster {
class controller;
}

namespace experimental::cloud_topics::l1 {

// The control plane of cloud topics is responsible for creating
// domains.
//
// There is only a single control plane per node and it runs on shard 0.
class domain_supervisor {
    class impl;

public:
    explicit domain_supervisor(cluster::controller*);
    domain_supervisor(const domain_supervisor&) = delete;
    domain_supervisor(domain_supervisor&&) = delete;
    domain_supervisor& operator=(const domain_supervisor&) = delete;
    domain_supervisor& operator=(domain_supervisor&&) = delete;
    ~domain_supervisor();

    ss::future<> start();
    ss::future<> stop();

private:
    std::unique_ptr<impl> _impl;
};

} // namespace experimental::cloud_topics::l1
