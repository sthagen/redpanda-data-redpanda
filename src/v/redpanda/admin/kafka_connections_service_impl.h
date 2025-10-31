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

#include "base/seastarx.h"
#include "container/priority_queue.h"
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "ssx/async_algorithm.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <cstddef>

namespace admin::detail {

struct connection_collector {
    virtual ~connection_collector() = default;
    virtual void add(proto::admin::kafka_connection conn) = 0;
    virtual ss::future<>
    add_all(chunked_vector<proto::admin::kafka_connection> conns) = 0;
    virtual chunked_vector<proto::admin::kafka_connection>
    extract_unordered() && = 0;
    virtual ss::future<chunked_vector<proto::admin::kafka_connection>>
    extract() && = 0;
    virtual size_t size() const = 0;
};

class unordered_collector : public connection_collector {
    chunked_vector<proto::admin::kafka_connection> _connections;
    size_t _limit;

public:
    explicit unordered_collector(size_t limit)
      : _limit(limit) {}

    void add(proto::admin::kafka_connection conn) final {
        if (_connections.size() < _limit) {
            _connections.emplace_back(std::move(conn));
        }
    }

    ss::future<>
    add_all(chunked_vector<proto::admin::kafka_connection> conns) final {
        auto to_add_count = std::min(
          conns.size(), _limit - _connections.size());
        auto insert_range = std::ranges::subrange(
          conns.begin(), conns.begin() + to_add_count);
        _connections.reserve(_connections.size() + insert_range.size());
        co_await ssx::async_for_each(insert_range, [this](auto& conn) {
            _connections.emplace_back(std::move(conn));
        });
    }

    chunked_vector<proto::admin::kafka_connection> extract_unordered()
      && final {
        return std::move(_connections);
    }

    ss::future<chunked_vector<proto::admin::kafka_connection>> extract()
      && final {
        co_return std::move(_connections);
    };

    size_t size() const final { return _connections.size(); }
};

template<typename Comparator>
class ordered_collector : public connection_collector {
    // Invert the order here to get the min-k instead of the max-k
    chunked_bounded_priority_queue<
      proto::admin::kafka_connection,
      ::detail::invert_comparator<Comparator>>
      _pq;

public:
    ordered_collector(size_t limit, Comparator comp)
      : _pq(limit, ::detail::invert_comparator<Comparator>(std::move(comp))) {}

    void add(proto::admin::kafka_connection conn) final {
        _pq.push(std::move(conn));
    }

    ss::future<>
    add_all(chunked_vector<proto::admin::kafka_connection> conns) final {
        co_await _pq.async_push_range(std::move(conns));
    }

    chunked_vector<proto::admin::kafka_connection> extract_unordered()
      && final {
        return std::move(_pq).extract_heap();
    }

    ss::future<chunked_vector<proto::admin::kafka_connection>> extract()
      && final {
        return std::move(_pq).async_extract_sorted();
    }

    size_t size() const final { return _pq.size(); }
};

using make_local_collector_t
  = ss::noncopyable_function<std::unique_ptr<connection_collector>(size_t)>;

struct connection_gather_result {
    chunked_vector<proto::admin::kafka_connection> connections;
    size_t total_matching_count;
};

} // namespace admin::detail
