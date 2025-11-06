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

#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka::client {

class topic_cache {
    struct partition_data {
        model::node_id leader;
        kafka::leader_epoch leader_epoch{invalid_leader_epoch};
    };

    struct topic_data {
        chunked_hash_map<model::partition_id, partition_data> partitions;
        kafka::topic_authorized_operations authorized_operations
          = kafka::topic_authorized_operations_not_set;
        int16_t replication_factor;
        std::optional<model::topic_id> topic_id;
        ss::lowres_clock::time_point last_seen_time;
    };

    using topics_t = chunked_hash_map<model::topic, topic_data>;

public:
    topic_cache() = default;
    topic_cache(const topic_cache&) = delete;
    topic_cache(topic_cache&&) = default;
    topic_cache& operator=(const topic_cache&) = delete;
    topic_cache& operator=(topic_cache&&) = delete;
    ~topic_cache() noexcept = default;

    explicit topic_cache(std::chrono::milliseconds topic_timeout)
      : _topics{}
      , _topic_timeout{std::move(topic_timeout)} {}

    /// \brief Apply the given metadata response.
    void apply(const chunked_vector<metadata_response::topic>& topics);

    /// \brief Obtain the leader for the given topic-partition
    std::optional<model::node_id> leader(model::topic_partition_view) const;
    std::optional<kafka::leader_epoch>
      leader_epoch(model::topic_partition_view) const;

    /// \brief A view of all known topics
    auto topics() const { return std::views::keys(_topics); }

    std::optional<model::topic_id> topic_id_for_name(model::topic_view) const;

    std::optional<kafka::topic_authorized_operations>
    authorized_operations_for_topic(model::topic_view tp) const;

    std::optional<int32_t> partition_count(model::topic_view tp) const;

    std::optional<int16_t> replication_factor(model::topic_view tp) const;

    const topics_t& cache() const noexcept;

    void set_topic_timeout(std::chrono::milliseconds s) { _topic_timeout = s; }

private:
    /// \brief merges info from new_topics and _topics
    void merge_topics(topics_t new_topics);

    /// \brief remove topics that haven't been seen for _topic_timeout or more
    void remove_timeout_topics();

    /// \brief merges cached and update topic data
    static topic_data merge_topic_data(topic_data&&, topic_data&&);

    /// \brief Cache of topic information.
    topics_t _topics;

    /// \brief If a topic hasn't been updated in this much time, it will be
    /// considered stale and removed
    std::chrono::milliseconds _topic_timeout = std::chrono::seconds{60};
};

} // namespace kafka::client
