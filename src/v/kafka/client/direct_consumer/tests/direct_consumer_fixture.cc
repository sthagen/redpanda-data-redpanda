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

#include "kafka/client/direct_consumer/tests/direct_consumer_fixture.h"

#include "kafka/client/direct_consumer/fetcher.h"
#include "kafka/server/tests/produce_consume_utils.h"

ss::logger logger{"direct-consumer-fixture"};

namespace kafka::client::tests {
kafka::client::connection_configuration
consumer_fixture::make_connection_config() {
    kafka::client::connection_configuration config;
    config.client_id = "test-client";
    config.initial_brokers
      = std::views::transform(
          instance_ids(), [this](model::node_id id) { return instance(id); })
        | std::views::transform(
          [](const redpanda_thread_fixture* f) { return f->kafka_port; })
        | std::views::transform(
          [](int port) { return net::unresolved_address("localhost", port); })
        | std::ranges::to<std::vector<net::unresolved_address>>();
    return config;
}

ss::future<std::unique_ptr<kafka::client::cluster>>
consumer_fixture::create_client_cluster(
  std::optional<std::unique_ptr<kafka::client::broker_factory>>
    broker_factory) {
    auto cluster = [&]() {
        if (broker_factory.has_value()) {
            vassert(
              broker_factory.value() != nullptr,
              "expected non-null broker factory if provided");
            return std::make_unique<kafka::client::cluster>(
              make_connection_config(), std::move(broker_factory.value()));
        }
        return std::make_unique<kafka::client::cluster>(
          make_connection_config());
    }();
    co_await cluster->start();
    co_return cluster;
}

topic_assignment consumer_fixture::make_assignment(
  const model::topic& topic,
  std::vector<int> partitions,
  std::optional<kafka::offset> initial_offset) {
    topic_assignment assignment;
    assignment.topic = topic;
    for (const auto& p : partitions) {
        partition_assignment part;
        part.partition_id = model::partition_id(p);
        part.next_offset = initial_offset;
        assignment.partitions.push_back(part);
    }
    return assignment;
}

model::node_id consumer_fixture::get_partition_leader(const model::ntp& ntp) {
    std::optional<model::node_id> leader_id;
    model::timeout_clock::time_point deadline = model::timeout_clock::now()
                                                + 10s;
    while (!leader_id) {
        if (model::timeout_clock::now() > deadline) {
            throw std::runtime_error(
              fmt::format("Timeout while waiting for leader for {}", ntp));
        }
        leader_id = instance(model::node_id{0})
                      ->app.controller->get_partition_leaders()
                      .local()
                      .get_leader(ntp);

        ss::sleep(100ms).get();
    }
    return leader_id.value();
}

void consumer_fixture::produce_to_partition(
  const model::topic& topic, int partition, size_t record_count) {
    model::ntp ntp(
      model::kafka_namespace, topic, model::partition_id(partition));

    auto leader_id = get_partition_leader(ntp);

    vlog(
      logger.debug,
      "[broker: {}] Produce {} records to {}",
      leader_id,
      record_count,
      ntp);

    ::tests::kafka_produce_transport producer(
      instance(leader_id)->make_kafka_client().get());
    producer.start().get();
    auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

    auto records = ::tests::kv_t::sequence(0, record_count, partition);
    producer
      .produce_to_partition(topic, model::partition_id(partition), records)
      .get();
}

chunked_hash_map<model::topic_partition, chunked_vector<model::record_batch>>
consumer_fixture::fetch_until_empty(direct_consumer& consumer) {
    chunked_hash_map<
      model::topic_partition,
      chunked_vector<model::record_batch>>
      ret;

    while (true) {
        auto fetched = consumer.fetch_next(1000ms).get();

        if (fetched.value().empty()) {
            break;
        }
        for (auto& topic_data : fetched.value()) {
            for (auto& partition_data : topic_data.partitions) {
                auto& batches = ret[model::topic_partition(
                  topic_data.topic, partition_data.partition_id)];

                std::ranges::move(
                  partition_data.data, std::back_inserter(batches));
            }
        }
    }
    return ret;
}

void consumer_fixture::assign_partitions(topic_assignment assgn) {
    consumer
      ->assign_partitions(
        chunked_vector<topic_assignment>::single(std::move(assgn)))
      .get();
}

void consumer_fixture::unassign_partition(model::topic_partition tp) {
    consumer
      ->unassign_partitions(
        chunked_vector<model::topic_partition>::single(std::move(tp)))
      .get();
}

void consumer_fixture::unassign_topic(model::topic topic) {
    consumer
      ->unassign_topics(chunked_vector<model::topic>::single(std::move(topic)))
      .get();
}

// shuffle leadership, wait for leadership change to become visible to the
// test
void consumer_fixture::wait_for_visible_leadership_shuffle(
  const model::ntp& ntp) {
    constexpr auto leadership_swap_wait = 300ms;
    const auto original_leader = get_leader(ntp);
    auto current_leader = original_leader;

    shuffle_leadership(ntp).get();

    for (const auto iteration : std::array{0, 1, 2, 3, 4, 5}) {
        logger.info("polling for leadership change iteration: {}", iteration);
        current_leader = get_leader(ntp);
        ss::sleep(leadership_swap_wait).get();
        if (current_leader != original_leader) {
            break;
        }
    }
    ASSERT_NE(current_leader, original_leader)
      << "never detected leadership change, test precondition failed";
}

void basic_consumer_fixture::SetUp() {
    create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});
    auto* rp = instance(model::node_id{0});
    wait_for_all_members(3s).get();
    rp->add_topic({model::kafka_namespace, topic}, 3, std::nullopt, 3).get();
    cluster = create_client_cluster().get();
    consumer = std::make_unique<kafka::client::direct_consumer>(
      *cluster,
      direct_consumer::configuration{
        .with_sessions = fetch_sessions_enabled{
          GetParam() == session_config::with_sessions}});
    consumer->start().get();
}

void basic_consumer_fixture::TearDown() {
    consumer->stop().get();
    cluster->stop().get();
}

void basic_consumer_fixture::maybe_toggle_fetch_sessions() {
    if (GetParam() == session_config::toggle_sessions) {
        consumer->update_configuration(
          direct_consumer::configuration{
            .with_sessions = fetch_sessions_enabled::yes});
    }
}

} // namespace kafka::client::tests
