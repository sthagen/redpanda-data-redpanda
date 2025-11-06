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

#include "container/chunked_vector.h"
#include "kafka/client/topic_cache.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

using namespace kafka;
using namespace kafka::client;
using namespace std::chrono_literals;

metadata_response::topic make_response_topic(
  model::topic name,
  int n_partitions,
  std::optional<model::topic_id> id = std::nullopt,
  std::optional<kafka::leader_epoch> leader_epoch = std::nullopt) {
    metadata_response::topic ret;
    ret.name = name;
    ret.topic_id = id.value_or(model::topic_id::create());

    for (int i_part = 0; i_part < n_partitions; ++i_part) {
        ret.partitions.push_back(
          metadata_response::partition{
            .partition_index = model::partition_id{i_part},
            .leader_epoch = leader_epoch.value_or(
              kafka::leader_epoch{i_part})});
    }

    return ret;
}

TEST(TopicCache, TestCacheUpdate) {
    topic_cache cache;

    const auto get_leader_epoch = [](const auto& data, int id) {
        return data.partitions.find(model::partition_id{id})
          ->second.leader_epoch;
    };

    const auto topic_a = model::topic{"topic-a"};
    const auto topic_b = model::topic{"topic-b"};
    const auto topic_a_id = model::topic_id::create();
    const auto topic_b_id = model::topic_id::create();

    // Check state after initial update.
    {
        chunked_vector<metadata_response::topic> init_update;
        init_update.push_back(make_response_topic(topic_a, 3, topic_a_id));
        init_update.push_back(make_response_topic(topic_b, 5, topic_b_id));

        cache.apply(init_update);

        ASSERT_TRUE(std::ranges::contains(cache.topics(), topic_a));
        ASSERT_TRUE(std::ranges::contains(cache.topics(), topic_b));

        const auto& topic_a_data = cache.cache().find(topic_a)->second;
        ASSERT_EQ(topic_a_data.topic_id, topic_a_id);
        ASSERT_EQ(topic_a_data.partitions.size(), 3);
        for (const auto& [par_id, par_data] : topic_a_data.partitions) {
            ASSERT_EQ(par_data.leader_epoch, kafka::leader_epoch{par_id});
        }

        const auto& topic_b_data = cache.cache().find(topic_b)->second;
        ASSERT_EQ(topic_b_data.topic_id, topic_b_id);
        ASSERT_EQ(topic_b_data.partitions.size(), 5);
        for (const auto& [par_id, par_data] : topic_b_data.partitions) {
            ASSERT_EQ(par_data.leader_epoch, kafka::leader_epoch{par_id});
        }
    }

    // Update topic-b with the same topic id
    {
        chunked_vector<metadata_response::topic> update_topic_b;
        update_topic_b.push_back(
          make_response_topic(topic_b, 6, topic_b_id, kafka::leader_epoch{2}));

        cache.apply(update_topic_b);

        ASSERT_TRUE(std::ranges::contains(cache.topics(), topic_a));
        ASSERT_TRUE(std::ranges::contains(cache.topics(), topic_b));

        // topic a state should be as before
        const auto& topic_a_data = cache.cache().find(topic_a)->second;
        ASSERT_EQ(topic_a_data.topic_id, topic_a_id);
        ASSERT_EQ(topic_a_data.partitions.size(), 3);
        for (const auto& [par_id, par_data] : topic_a_data.partitions) {
            ASSERT_EQ(par_data.leader_epoch, kafka::leader_epoch{par_id});
        }

        // topic b should have updated partitions for those with leader epoch
        // higher than before
        const auto& topic_b_data = cache.cache().find(topic_b)->second;
        ASSERT_EQ(topic_b_data.topic_id, topic_b_id);
        ASSERT_EQ(topic_b_data.partitions.size(), 6);
        ASSERT_EQ(get_leader_epoch(topic_b_data, 0), 2);
        ASSERT_EQ(get_leader_epoch(topic_b_data, 1), 2);
        ASSERT_EQ(get_leader_epoch(topic_b_data, 2), 2);
        ASSERT_EQ(get_leader_epoch(topic_b_data, 3), 3);
        ASSERT_EQ(get_leader_epoch(topic_b_data, 4), 4);
        ASSERT_EQ(get_leader_epoch(topic_b_data, 5), 2);
    }

    // Update topic b with different topic id
    {
        chunked_vector<metadata_response::topic> update_topic_b;
        update_topic_b.push_back(make_response_topic(topic_b, 3));

        cache.apply(update_topic_b);

        ASSERT_TRUE(std::ranges::contains(cache.topics(), topic_b));

        const auto& topic_b_data = cache.cache().find(topic_b)->second;
        ASSERT_NE(topic_b_data.topic_id, topic_b_id);
        ASSERT_EQ(topic_b_data.partitions.size(), 3);
        for (const auto& [par_id, par_data] : topic_b_data.partitions) {
            ASSERT_EQ(par_data.leader_epoch, kafka::leader_epoch{par_id});
        }
    }
}

TEST(TopicCache, TestCacheUpdateStaleTopic) {
    topic_cache cache;
    cache.set_topic_timeout(1s);

    const auto topic_a = model::topic{"topic-a"};
    const auto topic_b = model::topic{"topic-b"};
    const auto topic_a_id = model::topic_id::create();
    const auto topic_b_id = model::topic_id::create();

    // Set up cache with both topics.
    chunked_vector<metadata_response::topic> init_update;
    init_update.push_back(make_response_topic(topic_a, 3, topic_a_id));
    init_update.push_back(make_response_topic(topic_b, 5, topic_b_id));

    cache.apply(init_update);

    ASSERT_TRUE(std::ranges::contains(cache.topics(), topic_a));
    ASSERT_TRUE(std::ranges::contains(cache.topics(), topic_b));

    // Wait enough time for the topics to be considered stale
    ss::sleep(2s).get();

    // Update only topic-a
    chunked_vector<metadata_response::topic> update_topic_a;
    update_topic_a.push_back(make_response_topic(topic_a, 3, topic_a_id));

    cache.apply(update_topic_a);

    ASSERT_TRUE(std::ranges::contains(cache.topics(), topic_a));
    ASSERT_FALSE(std::ranges::contains(cache.topics(), topic_b));
}
