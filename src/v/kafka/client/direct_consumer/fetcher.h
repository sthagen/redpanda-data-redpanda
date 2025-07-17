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
#include "container/intrusive_list_helpers.h"
#include "kafka/client/direct_consumer/api_types.h"
#include "kafka/client/direct_consumer/data_queue.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/list_offset.h"
#include "model/fundamental.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>
namespace kafka::client {
class direct_consumer;

struct fetch_session_state {
    enum class state {
        none,
        need_full_fetch,
        incremental_fetch,
        needs_close,
    };
    kafka::fetch_session_id _fetch_session_id{kafka::invalid_fetch_session_id};
    kafka::fetch_session_epoch _fetch_session_epoch{
      kafka::initial_fetch_session_epoch};

    void update_fetch_session(kafka::fetch_session_id id) {
        _fetch_session_id = id;
        _fetch_session_epoch++;
    }
};

/**
 * Class responsible for fetching data from a single broker. It is maintaining a
 * list of partitions and corresponding fetch offsets. The fetcher loop
 * constantly querying the broker for data and updates the fetch offsets based
 * on the received data. Fetcher is also responsible for fetching offsets to
 * apply the reset policy.
 *
 * Fetcher put the fetched data into the parent consumer's data queue.
 *
 * Fetcher locks the assignment state while changing assignments, preparing the
 * fetch request and processing the response. Every time the partition
 * assignment is updated its corresponding assignment epoch is incremented.
 * Partition fetch responses with stale assignment epoch are ignored. This
 * concurrency control mechanism guarantees that the fetcher will not update its
 * subscriptions with the stale information from the request when it was changed
 * more than once while it was waiting for the response.
 *
 *
 *
 * TODO:
 * - support incremental fetches
 * - support leader epochs
 * - support server side throttling and quotas
 */
class fetcher {
public:
    fetcher(direct_consumer* parent, model::node_id id);
    void start();

    ss::future<> stop();
    /**
     * Assign fetcher partitions, it can be used to assign new partitions or
     * update the fetch offsets for the already assigned partitions.
     */
    ss::future<> assign_partition(
      model::topic_partition_view, std::optional<kafka::offset>);
    /**
     * Unassign partition from the fetcher, it will stop fetching data for the
     * partition and remove it from the fetcher state. After the unassignment
     * the fetcher will not return any data for the partition.
     *
     * Returned offset can be used to reassign the partition later to different
     * fetcher to continue reading from the same offset.
     */

    ss::future<std::optional<kafka::offset>>
      unassign_partition(model::topic_partition_view);

    bool is_idle() const { return _partitions.empty(); }

private:
    using assignment_epoch = named_type<uint64_t, struct fetcher_epoch_tag>;
    struct partition_fetch_state {
        model::partition_id partition_id;
        std::optional<kafka::offset> fetch_offset;
        kafka::offset high_watermark;
        leader_epoch current_leader_epoch{kafka::invalid_leader_epoch};
        intrusive_list_hook _hook;
        assignment_epoch assignment_epoch{0};

        bool include_in_fetch_request() const {
            return fetch_offset.has_value();
        }
    };
    using state_list
      = intrusive_list<partition_fetch_state, &partition_fetch_state::_hook>;
    struct partitions_to_process {
        model::topic topic;
        state_list to_include_in_fetch;
        state_list to_list_offsets;

        bool empty() const {
            return to_include_in_fetch.empty() && to_list_offsets.empty();
        }
    };
    struct partitions_with_epoch {
        topic_partition_map<assignment_epoch> assignment_epochs;
        chunked_vector<partitions_to_process> partitions;
    };
    struct fetch_response_content {
        chunked_vector<fetched_topic_data> topics;
        size_t total_bytes{0};
        bool needs_metadata_update{false};
    };

    static assignment_epoch find_assignment_epoch(
      const model::topic& topic,
      model::partition_id partition,
      const topic_partition_map<assignment_epoch>& epochs);

    ss::future<api_version> get_fetch_request_version() const;
    ss::future<api_version> get_list_offsets_request_version() const;
    ss::future<> do_fetch();
    ss::future<partitions_with_epoch> collect_partitions();
    ss::future<kafka::error_code> maybe_initialise_fetch_offsets(
      const chunked_vector<partitions_to_process>&,
      const topic_partition_map<assignment_epoch>& epochs);
    ss::future<fetch_request>
      make_fetch_request(chunked_vector<partitions_to_process>);

    ss::future<kafka_result<fetch_response_content>> process_fetch_response(
      fetch_response resp, const topic_partition_map<assignment_epoch>& epochs);
    /**
     * Returns false if the partition was not found or the fetch offset was
     * not updated.
     * This indicates that the fetch response should be ignored.
     */
    bool maybe_update_fetch_offset(
      const model::topic&,
      model::partition_id,
      kafka::offset,
      assignment_epoch);

    ss::future<kafka_result<chunked_vector<topic_partition_offsets>>>
      do_list_offsets(list_offsets_request);

    data_queue& queue();
    prefix_logger& logger();
    assignment_epoch next_epoch() { return ++_epoch; }

    direct_consumer* _parent;
    model::node_id _id;
    fetch_session_state _session_state;
    topic_partition_map<partition_fetch_state> _partitions;
    ss::condition_variable _partitions_updated;
    ss::gate _gate;
    mutex _state_lock;
    assignment_epoch _epoch{0};
    ss::abort_source _as;
};
} // namespace kafka::client
