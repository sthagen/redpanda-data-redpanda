/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "ssx/actor.h"

#include <seastar/core/future.hh>

#include <memory>

namespace cloud_topics::l1 {

class metastore;
class flush_loop;

// Manages a loop to flush the metastore that runs only on leadership of
// partition 0 of the metastore topic. Under the hood, each flush will request
// flushes of each partition, so it's sufficient to only run this on one
// partition.
//
// TODO: it may be worth having each domain independently flush (e.g. if
// there's a lull in traffic), and then have this loop request flushes if there
// hasn't been a domain flush within some time bound. Giving some control to
// individual domain could help us avoid potential added latencies that may
// come from flushing while the domain is serving a high volume of requests.
class flush_loop_manager
  : public ssx::actor<
      ss::bool_class<struct flush_needs_loop_tag>,
      1,
      ssx::overflow_policy::drop_oldest> {
public:
    using needs_loop = ss::bool_class<struct flush_needs_loop_tag>;

    explicit flush_loop_manager(metastore* metastore);
    ~flush_loop_manager() override;

    // Enqueues a reset of the loop such that eventually a flush_loop will be
    // running if needs_loop is true, or not running if false.
    void enqueue_loop_reset(needs_loop needs);

    ss::future<> stop() override;

protected:
    ss::future<> process(needs_loop needs) override;
    void on_error(std::exception_ptr ex) noexcept override;

private:
    ss::future<> reset_flush_loop(needs_loop needs);

    metastore* metastore_;
    std::unique_ptr<flush_loop> flush_loop_;
};

} // namespace cloud_topics::l1
