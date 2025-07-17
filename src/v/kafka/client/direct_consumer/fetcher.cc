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
#include "kafka/client/direct_consumer/fetcher.h"

#include "kafka/client/direct_consumer/data_queue.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/client/errors.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"

#include <seastar/core/sleep.hh>

namespace kafka::client {
static constexpr model::node_id client_replica_id{-1};
static constexpr std::chrono::milliseconds error_backoff(200);

data_queue& fetcher::queue() { return *_parent->_fetched_data_queue; }
prefix_logger& fetcher::logger() { return _parent->_cluster->logger(); }

fetcher::fetcher(direct_consumer* parent, model::node_id id)
  : _parent(parent)
  , _id(id)
  , _state_lock("fetcher/state") {}

void fetcher::start() {
    ssx::repeat_until_gate_closed(_gate, [this] { return do_fetch(); });
}

ss::future<> fetcher::stop() {
    vlog(logger().debug, "[broker: {}] Stopping fetcher", _id);
    _as.request_abort();
    _state_lock.broken();
    _partitions_updated.broken();
    return _gate.close();
}

ss::future<fetcher::partitions_with_epoch> fetcher::collect_partitions() {
    auto lock = co_await _state_lock.get_units();
    fetcher::partitions_with_epoch ret;
    ret.partitions.reserve(_partitions.size());
    ssx::async_counter cnt;
    for (auto& [topic, partitions] : _partitions) {
        partitions_to_process to_process;
        to_process.topic = topic;

        co_await ssx::async_for_each_counter(
          cnt, partitions, [&to_process, &ret](auto& p_fs) {
              partition_fetch_state& fetch_state = p_fs.second;
              ret.assignment_epochs[to_process.topic][fetch_state.partition_id]
                = fetch_state.assignment_epoch;
              if (!fetch_state.fetch_offset.has_value()) {
                  to_process.to_list_offsets.push_back(fetch_state);
                  return;
              }

              to_process.to_include_in_fetch.push_back(fetch_state);
          });
        if (!to_process.empty()) {
            ret.partitions.push_back(std::move(to_process));
        }
    }

    co_return ret;
}

ss::future<fetch_request>
fetcher::make_fetch_request(chunked_vector<partitions_to_process> to_process) {
    // TODO: handle api versions here
    // f.e. customize fetch request based on the version
    fetch_request req;
    req.data.replica_id = client_replica_id;
    req.data.isolation_level = _parent->_config.isolation_level;
    req.data.max_wait_ms = _parent->_config.max_wait_time;
    req.data.max_bytes = _parent->_config.max_fetch_size;
    req.data.min_bytes = _parent->_config.min_bytes;

    // TODO: support incremental fetches, for now we always use full fetch
    req.data.session_id = invalid_fetch_session_id;
    req.data.session_epoch = final_fetch_session_epoch;

    ssx::async_counter cnt;

    for (auto& topic_partitions : to_process) {
        fetch_topic f_topic;

        if (topic_partitions.to_include_in_fetch.empty()) {
            continue;
        }
        f_topic.topic = std::move(topic_partitions.topic);
        f_topic.partitions.reserve(topic_partitions.to_include_in_fetch.size());
        co_await ssx::async_for_each_counter(
          cnt,
          topic_partitions.to_include_in_fetch,
          [this, &f_topic](const partition_fetch_state& f_state) {
              fetch_partition f_partition;
              f_partition.partition = f_state.partition_id;
              f_partition.fetch_offset = kafka::offset_cast(
                *f_state.fetch_offset);
              f_partition.last_fetched_epoch = f_state.current_leader_epoch;
              f_partition.partition_max_bytes
                = _parent->_config.partition_max_bytes;
              f_topic.partitions.push_back(std::move(f_partition));
          });
        req.data.topics.push_back(std::move(f_topic));
    }

    co_return req;
}

namespace {

ss::future<chunked_vector<model::record_batch>>
reader_to_chunked_vector(kafka::batch_reader reader) {
    return model::consume_reader_to_chunked_vector(
      model::make_record_batch_reader<kafka::batch_reader>(std::move(reader)),
      model::no_timeout);
}
} // namespace

bool fetcher::maybe_update_fetch_offset(
  const model::topic& topic,
  model::partition_id partition_id,
  kafka::offset last_received,
  assignment_epoch assignment_epoch) {
    auto t_it = _partitions.find(topic);
    if (t_it == _partitions.end()) {
        return false;
    }
    auto p_it = t_it->second.find(partition_id);
    if (p_it == t_it->second.end()) {
        return false;
    }
    if (p_it->second.assignment_epoch != assignment_epoch) {
        vlog(
          logger().trace,
          "[broker: {}] Ignoring {}/{} reply, assignment epoch changed, "
          "request epoch: {}, current epoch: {}",
          _id,
          topic,
          partition_id,
          assignment_epoch,
          p_it->second.assignment_epoch);
        return false;
    }

    vlog(
      logger().trace,
      "[broker: {}] Updating {}/{} fetch offset to {}",
      _id,
      topic,
      partition_id,
      kafka::next_offset(last_received));
    p_it->second.fetch_offset = kafka::next_offset(last_received);
    return true;
}

ss::future<> fetcher::do_fetch() {
    bool needs_backoff = false;
    try {
        co_await _partitions_updated.wait(
          [this] { return !_partitions.empty(); });

        /**
         * Iterate once over all partitions that are assigned to the fetcher and
         * collect necessary actions. f.e. list offsets or include in fetch
         * request.
         */
        auto partitions_with_epochs = co_await collect_partitions();
        auto assignment_epochs = std::move(
          partitions_with_epochs.assignment_epochs);

        auto list_offsets_err = co_await maybe_initialise_fetch_offsets(
          partitions_with_epochs.partitions, assignment_epochs);
        /**
         */
        if (list_offsets_err != kafka::error_code::none) {
            vlog(
              logger().debug,
              "[broker: {}] list offsets error: {}",
              _id,
              list_offsets_err);
            if (is_retriable_error(list_offsets_err)) {
                needs_backoff = true;
            } else {
                // propagate not retriable error to the queue
                co_await queue().push_error(list_offsets_err);
            }
        }

        auto req = co_await make_fetch_request(
          std::move(partitions_with_epochs.partitions));

        if (_as.abort_requested()) {
            // if the abort was requested, we should not dispatch the request
            // and just return
            co_return;
        }
        // TODO: cache the version of the fetch request
        auto version = co_await get_fetch_request_version();
        auto response = co_await _parent->_cluster->dispatch_to(
          _id, std::move(req), version);
        auto fetch_result = co_await process_fetch_response(
          std::move(response), assignment_epochs);

        if (fetch_result.has_error()) {
            if (is_retriable_error(fetch_result.error())) {
                needs_backoff = true;
            } else {
                // propagate non retriable error to the queue
                co_await queue().push_error(fetch_result.error());
                co_return;
            }
        }

        auto fetch_result_value = std::move(fetch_result.value());
        if (fetch_result_value.needs_metadata_update) {
            // if we need to update metadata, we should do it
            // so that we can retry fetching the partitions later
            needs_backoff = true;
        }

        if (!fetch_result_value.topics.empty()) {
            co_await queue().push(
              std::move(fetch_result_value.topics),
              fetch_result_value.total_bytes);
        }

    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            // if the exception is a shutdown exception, we should not log it
            // and just return
            co_return;
        }
        vlog(
          logger().warn,
          "error encountered while fetching from broker with id: {} - {}",
          _id,
          std::current_exception());
        needs_backoff = true;
    }
    if (needs_backoff) {
        co_await _parent->_cluster->request_metadata_update();
        co_await ss::sleep_abortable(error_backoff, _as);
    }
}

fetcher::assignment_epoch fetcher::find_assignment_epoch(
  const model::topic& topic,
  model::partition_id partition,
  const topic_partition_map<assignment_epoch>& epochs) {
    auto it = epochs.find(topic);
    vassert(
      it != epochs.end(), "assignment epoch for topic {} not found", topic);

    auto p_it = it->second.find(partition);
    vassert(
      p_it != it->second.end(),
      "assignment epoch for partition {}/{} not found",
      topic,
      partition);

    return p_it->second;
}

ss::future<kafka_result<fetcher::fetch_response_content>>
fetcher::process_fetch_response(
  fetch_response resp, const topic_partition_map<assignment_epoch>& epochs) {
    if (resp.data.error_code != kafka::error_code::none) {
        co_return resp.data.error_code;
    }
    // Hold the lock here as the fetch response processing updates the fetch
    // offsets, we do not want this to interfere with assignment updates.
    auto lock = co_await _state_lock.get_units();

    fetch_response_content result;
    result.topics.reserve(resp.data.responses.size());
    for (auto& topic_response : resp.data.responses) {
        if (topic_response.partitions.empty()) {
            // no partitions in the response, skip it
            continue;
        }
        fetched_topic_data topic_data;
        topic_data.topic = std::move(topic_response.topic);
        topic_data.partitions.reserve(topic_response.partitions.size());

        for (auto& part_response : topic_response.partitions) {
            fetched_partition_data part_data;
            part_data.error = part_response.error_code;
            part_data.partition_id = part_response.partition_index;

            if (part_response.error_code != kafka::error_code::none) {
                if (is_retriable_error(part_response.error_code)) {
                    vlog(
                      logger().debug,
                      "[broker: {}] {}/{} retriable fetch error: {}",
                      _id,
                      topic_data.topic,
                      part_data.partition_id,
                      part_response.error_code);

                    result.needs_metadata_update = true;
                    // skip partition in the result, but mark that we
                    // need to update metadata
                    // so that we can retry fetching it later
                    continue;
                }
                vlog(
                  logger().warn,
                  "[broker: {}] {}/{} fetch error: {}",
                  _id,
                  topic_data.topic,
                  part_data.partition_id,
                  part_response.error_code);
            } else {
                part_data.high_watermark = model::offset_cast(
                  part_response.high_watermark);
                part_data.last_stable_offset = model::offset_cast(
                  part_response.last_stable_offset);
                part_data.leader_epoch
                  = part_response.current_leader.leader_epoch;
                part_data.aborted_transactions = std::move(
                  part_response.aborted_transactions);

                vlog(
                  logger().trace,
                  "[broker: {}] topic: {}, partition fetch response: {}",
                  _id,
                  topic_data.topic,
                  part_response);

                if (
                  !part_response.records.has_value()
                  || part_response.records->is_end_of_stream()) {
                    continue;
                }
                topic_data.total_bytes += part_response.records->size_bytes();
                part_data.data = co_await reader_to_chunked_vector(
                  std::move(part_response.records.value()));

                maybe_update_fetch_offset(
                  topic_data.topic,
                  part_data.partition_id,
                  model::offset_cast(part_data.data.back().last_offset()),
                  find_assignment_epoch(
                    topic_data.topic, part_data.partition_id, epochs));
            }
            topic_data.partitions.push_back(std::move(part_data));
        }
        result.total_bytes += topic_data.total_bytes;
        if (topic_data.partitions.empty()) {
            continue;
        }
        result.topics.push_back(std::move(topic_data));
    }

    co_return result;
}

namespace {
model::timestamp timestamp_for_offset_reset_policy(offset_reset_policy policy) {
    switch (policy) {
    case offset_reset_policy::earliest:
        return list_offsets_request::earliest_timestamp;
    case offset_reset_policy::latest:
        return list_offsets_request::latest_timestamp;
    }
}
} // namespace

ss::future<kafka::error_code> fetcher::maybe_initialise_fetch_offsets(
  const chunked_vector<partitions_to_process>& partitions,
  const topic_partition_map<assignment_epoch>& epochs) {
    const auto timestamp = timestamp_for_offset_reset_policy(
      _parent->_config.reset_policy);

    list_offsets_request req;
    req.data.topics.reserve(partitions.size());
    for (auto& topic_partitions : partitions) {
        if (topic_partitions.to_list_offsets.empty()) {
            // no partitions to fetch offsets for
            continue;
        }
        list_offset_topic l_topic;
        l_topic.name = topic_partitions.topic;
        l_topic.partitions.reserve(topic_partitions.to_list_offsets.size());
        for (auto& fetch_state : topic_partitions.to_list_offsets) {
            // we need to fetch the offset for this partition
            list_offset_partition l_partition;
            l_partition.partition_index = fetch_state.partition_id;
            l_partition.timestamp = timestamp;
            l_partition.current_leader_epoch = fetch_state.current_leader_epoch;

            l_topic.partitions.push_back(std::move(l_partition));
        }

        req.data.topics.push_back(std::move(l_topic));
    }

    if (req.data.topics.empty()) {
        // no partitions to fetch offsets for
        co_return error_code::none;
    }

    auto offsets = co_await do_list_offsets(std::move(req));
    if (offsets.has_error()) {
        if (is_retriable_error(offsets.error())) {
            vlog(
              logger().info,
              "list_offsets request failed with retriable error: {}",
              offsets.error());
        } else {
            vlog(
              logger().warn,
              "list_offsets request failed with an error: {}",
              offsets.error());
        }
        co_return offsets.error();
    }
    // TODO: what should we do with the error code in the response?
    kafka::error_code error = kafka::error_code::none;
    for (auto& topic : offsets.value()) {
        for (auto& partition_offset : topic.offsets) {
            if (partition_offset.error_code != kafka::error_code::none) {
                if (is_retriable_error(partition_offset.error_code)) {
                    vlog(
                      logger().debug,
                      "[broker: {}] {}/{} retriable list_offsets error: {}",
                      _id,
                      topic.topic,
                      partition_offset.partition_id,
                      partition_offset.error_code);
                }
                // Only overwrite if we don't have an error, or if current error
                // is retriable but new error is not retriable
                if (error == kafka::error_code::none ||
                  (is_retriable_error(error) && !is_retriable_error(partition_offset.error_code))) {
                    error = partition_offset.error_code;
                }
                continue;
            }

            auto t_it = _partitions.find(topic.topic);
            if (t_it == _partitions.end()) {
                continue;
            }
            auto p_it = t_it->second.find(partition_offset.partition_id);
            if (p_it == t_it->second.end()) {
                continue;
            }
            auto request_epoch = find_assignment_epoch(
              topic.topic, partition_offset.partition_id, epochs);
            if (
              find_assignment_epoch(
                topic.topic, partition_offset.partition_id, epochs)
              != p_it->second.assignment_epoch) {
                vlog(
                  logger().trace,
                  "[broker: {}] Skipping partition {}/{} list offset response "
                  "as assignment epoch has changed. request_epoch: {}, "
                  "current_epoch: {}",
                  _id,
                  topic.topic,
                  partition_offset.partition_id,
                  partition_offset.offset,
                  request_epoch,
                  p_it->second.assignment_epoch);
            }
            vlog(
              logger().info,
              "[broker: {}] Resetting partition {}/{} fetch offset to: {}",
              _id,
              topic.topic,
              partition_offset.partition_id,
              partition_offset.offset);
            p_it->second.fetch_offset = partition_offset.offset;
        }
    }

    co_return error;
}

ss::future<api_version> fetcher::get_fetch_request_version() const {
    auto version = co_await _parent->_cluster->supported_api_versions(
      _id, kafka::fetch_api::key);
    if (version) {
        co_return std::min(version->max, kafka::fetch_api::max_valid);
    }
    // if the version is not supported, we fallback to the minimum version
    // which is 1, this is the first version of the Fetch API that use the new
    // batch format
    co_return api_version{1};
}

ss::future<api_version> fetcher::get_list_offsets_request_version() const {
    auto version = co_await _parent->_cluster->supported_api_versions(
      _id, kafka::list_offsets_api::key);
    if (version) {
        co_return std::min(version->max, kafka::list_offsets_api::max_valid);
    }
    co_return kafka::list_offsets_api::min_valid;
}

ss::future<> fetcher::assign_partition(
  model::topic_partition_view tp, std::optional<kafka::offset> offset) {
    auto lock = co_await _state_lock.get_units();
    vlog(
      logger().debug,
      "[broker: {}] Assigned partition: {} with offset: {}",
      _id,
      tp,
      offset);

    _partitions[tp.topic][tp.partition] = partition_fetch_state{
      .partition_id = tp.partition,
      .fetch_offset = offset,
      .assignment_epoch = next_epoch(),
    };

    _partitions_updated.signal();
    co_return;
}
ss::future<std::optional<kafka::offset>>
fetcher::unassign_partition(model::topic_partition_view tp_v) {
    auto lock = co_await _state_lock.get_units();
    auto it = _partitions.find(tp_v.topic);
    if (it == _partitions.end()) {
        // partition not found, nothing to unassign
        co_return std::nullopt;
    }
    auto& partitions = it->second;
    auto p_it = partitions.find(tp_v.partition);
    if (p_it == partitions.end()) {
        // partition not found, nothing to unassign
        co_return std::nullopt;
    }
    vlog(
      logger().debug,
      "[broker: {}] Removing partition: {} assignment",
      _id,
      tp_v);
    // TODO: for fetch sessions we will need to mark partitions to deletion so
    // they can be removed from the fetch session
    auto fetch_offset = p_it->second.fetch_offset;
    partitions.erase(p_it);
    if (partitions.empty()) {
        // if there are no partitions left for this topic, remove the topic
        _partitions.erase(it);
    }

    _partitions_updated.signal();
    co_return fetch_offset;
}

ss::future<kafka_result<chunked_vector<topic_partition_offsets>>>
fetcher::do_list_offsets(list_offsets_request req) {
    try {
        auto version = co_await get_list_offsets_request_version();
        auto reply = co_await _parent->_cluster->dispatch_to(
          _id, std::move(req), version);
        chunked_vector<topic_partition_offsets> offsets;
        offsets.reserve(reply.data.topics.size());

        for (auto& topic : reply.data.topics) {
            topic_partition_offsets topics;
            topics.topic = std::move(topic.name);
            topics.offsets.reserve(topic.partitions.size());
            for (auto& partition : topic.partitions) {
                topics.offsets.push_back(partition_offset{
                  .partition_id = partition.partition_index,
                  .error_code = partition.error_code,
                  .leader_epoch = partition.leader_epoch,
                  .offset = model::offset_cast(partition.offset),
                });
            }
            offsets.push_back(std::move(topics));
        }
        co_return offsets;
    } catch (const broker_error& e) {
        vlog(
          logger().warn,
          "list_offsets request to broker {} failed with broker error: {}",
          _id,
          e);
        co_return e.error;
    } catch (...) {
        vlog(
          logger().warn,
          "list_offsets request to broker {} failed with an error: {}",
          _id,
          std::current_exception());
        co_return kafka::error_code::unknown_server_error;
    }
}

} // namespace kafka::client
