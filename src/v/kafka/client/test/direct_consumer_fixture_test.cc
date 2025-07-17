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

#include "cluster/tests/cluster_test_fixture.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "redpanda/tests/fixture.h"

#include <seastar/util/defer.hh>
using namespace kafka::client;
class consumer_fixture
  : public cluster_test_fixture
  , public ::testing::Test {
public:
    kafka::client::connection_configuration make_connection_config() {
        kafka::client::connection_configuration config;
        config.client_id = "test-client";
        config.initial_brokers
          = std::views::transform(
              instance_ids(),
              [this](model::node_id id) { return instance(id); })
            | std::views::transform(
              [](const redpanda_thread_fixture* f) { return f->kafka_port; })
            | std::views::transform([](int port) {
                  return net::unresolved_address("localhost", port);
              })
            | std::ranges::to<std::vector<net::unresolved_address>>();
        return config;
    }

    ss::future<std::unique_ptr<kafka::client::cluster>>
    create_client_cluster() {
        auto cluster = std::make_unique<kafka::client::cluster>(
          make_connection_config());
        co_await cluster->start();
        co_return cluster;
    }

    topic_assignment make_assignment(
      const model::topic& topic,
      std::vector<int> partitions,
      std::optional<kafka::offset> initial_offset = std::nullopt) {
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

    model::node_id get_partition_leader(const model::ntp& ntp) {
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

    void produce_to_partition(
      const model::topic& topic, int partition, size_t record_count) {
        model::ntp ntp(
          model::kafka_namespace, topic, model::partition_id(partition));

        auto leader_id = get_partition_leader(ntp);

        tests::kafka_produce_transport producer(
          instance(leader_id)->make_kafka_client().get());
        producer.start().get();
        auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

        auto records = tests::kv_t::sequence(0, record_count, partition);
        producer
          .produce_to_partition(topic, model::partition_id(partition), records)
          .get();
    }

    chunked_hash_map<
      model::topic_partition,
      chunked_vector<model::record_batch>>
    fetch_until_empty(direct_consumer& consumer) {
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
};

TEST_F(consumer_fixture, TestBasicConsumption) {
    create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});
    auto* rp = instance(model::node_id{0});
    wait_for_all_members(3s).get();
    model::topic topic{"test-topic"};
    rp->add_topic({model::kafka_namespace, topic}, 3, std::nullopt, 3).get();

    auto cluster = create_client_cluster().get();

    kafka::client::direct_consumer consumer(
      *cluster, direct_consumer::configuration{});
    consumer.start().get();
    auto d_stop = ss::defer([&cluster, &consumer] {
        consumer.stop().get();
        cluster->stop().get();
    });
    consumer
      .assign_partitions(chunked_vector<topic_assignment>::single(
        make_assignment(topic, {0, 1, 2})))
      .get();
    // no data should be available immediately, as the topic is empty
    for (int i = 0; i < 10; ++i) {
        auto fetched = consumer.fetch_next(100ms).get();
        ASSERT_TRUE(fetched.value().empty());
    }

    // produce some data
    produce_to_partition(topic, 0, 1000);
    produce_to_partition(topic, 1, 400);
    produce_to_partition(topic, 2, 20);
    auto fetched = fetch_until_empty(consumer);

    ASSERT_EQ(fetched.size(), 3);
    ASSERT_EQ(
      fetched[model::topic_partition(topic, model::partition_id(0))]
        .back()
        .last_offset(),
      model::offset(999));
    ASSERT_EQ(
      fetched[model::topic_partition(topic, model::partition_id(1))]
        .back()
        .last_offset(),
      model::offset(399));
    ASSERT_EQ(
      fetched[model::topic_partition(topic, model::partition_id(2))]
        .back()
        .last_offset(),
      model::offset(19));
    // produce again
    produce_to_partition(topic, 2, 1000);
    produce_to_partition(topic, 1, 400);
    produce_to_partition(topic, 0, 20);
    auto fetched_2 = fetch_until_empty(consumer);
    ASSERT_EQ(fetched_2.size(), 3);
    ASSERT_EQ(
      fetched_2[model::topic_partition(topic, model::partition_id(0))]
        .back()
        .last_offset(),
      model::offset(1019));
    ASSERT_EQ(
      fetched_2[model::topic_partition(topic, model::partition_id(1))]
        .back()
        .last_offset(),
      model::offset(799));
    ASSERT_EQ(
      fetched_2[model::topic_partition(topic, model::partition_id(2))]
        .back()
        .last_offset(),
      model::offset(1019));
}
