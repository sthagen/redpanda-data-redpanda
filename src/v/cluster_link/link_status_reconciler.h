/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#pragma once

#include "cluster_link/model/types.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

namespace cluster_link {

class link;
class link_registry;

/**
 * Runs on controller leader shard and is responsible for reconciling links and
 * their shadowing topic states.
 */
class link_status_reconciler {
public:
    explicit link_status_reconciler(
      link_registry* link_registry, ::model::term_id term)
      : _link_registry(link_registry)
      , _controller_term(term) {}

    ss::future<> start() noexcept;
    ss::future<> stop() noexcept;
    void reconcile();

private:
    class per_link_reconciler {
    public:
        explicit per_link_reconciler(
          link_registry&, model::id_t, ::model::term_id, ss::abort_source&);
        ss::future<> stop() noexcept;
        void notify_changes();

    private:
        ss::future<> reconcile_status_changes();
        bool has_pending_reconciliations() const;

        ss::future<> try_finish_failover(const ::model::topic&) noexcept;
        ss::condition_variable _cv;
        link_registry& _registry;
        model::id_t _link_id;
        ::model::term_id _term;
        ss::gate _gate;
        ss::abort_source _as;
        ss::optimized_optional<ss::abort_source::subscription> _as_sub;
    };
    chunked_hash_map<model::id_t, std::unique_ptr<per_link_reconciler>>
      _reconcilers;
    link_registry* _link_registry;
    ::model::term_id _controller_term;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cluster_link
