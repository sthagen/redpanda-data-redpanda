/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
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
