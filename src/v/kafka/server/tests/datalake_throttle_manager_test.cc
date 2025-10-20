
#include "config/mock_property.h"
#include "kafka/server/datalake_throttle_manager.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

using namespace testing;
using namespace std::chrono_literals;

struct shard_local_state {
    bool max_shares_assigned{false};
    size_t total_backlog{0};
    config::mock_property<std::chrono::milliseconds> producer_gc_threshold{
      1min};
    config::mock_property<std::chrono::milliseconds> max_throttle{30s};
};

struct IcebergThrottlingManagerTest : ::testing::Test {
    void SetUp() override {
        shard_state.start().get();
        storage_node.start(ss::sstring("/tmp"), ss::sstring("/tmp")).get();
        manager
          .start(
            [this] { return sample(); },
            std::ref(storage_node),
            ss::sharded_parameter(
              [this] { return shard_state.local().max_throttle.bind(); }),
            ss::sharded_parameter([this] {
                return shard_state.local().producer_gc_threshold.bind();
            }),
            ss::sharded_parameter(
              [] { return config::mock_binding<std::optional<double>>(0.01); }))
          .get();
        manager.invoke_on_all(&kafka::datalake_throttle_manager::start).get();
        storage_node.local().set_disk_metrics(
          storage::node::disk_type::data,
          storage::node::disk_space_info{
            .total = 8_GiB,
            .free = 6_GiB,
          });
    }

    void TearDown() override {
        manager.stop().get();
        storage_node.stop().get();
        shard_state.stop().get();
    }

    ss::future<kafka::datalake_throttle_manager::status> sample() {
        co_return kafka::datalake_throttle_manager::status{
          .max_shares_assigned = shard_state.local().max_shares_assigned,
          .total_translation_backlog = shard_state.local().total_backlog};
    }

    ss::future<> wait_for_producer_throttling(
      const std::optional<ss::sstring>& producer_id,
      std::chrono::milliseconds throttle_time_ms) {
        return tests::cooperative_spin_wait_with_timeout(
          5s, [this, producer_id, throttle_time_ms] {
              return manager.local()
                .maybe_throttle_producer(producer_id)
                .then([throttle_time_ms](std::chrono::milliseconds throttle) {
                    return throttle >= throttle_time_ms;
                });
          });
    }
    ss::future<>
    wait_for_no_throttling(const std::optional<ss::sstring>& producer_id) {
        return tests::cooperative_spin_wait_with_timeout(
          5s, [this, producer_id] {
              return manager.local()
                .maybe_throttle_producer(producer_id)
                .then([](std::chrono::milliseconds throttle_time_ms) {
                    return throttle_time_ms == 0ms;
                });
          });
    }
    ss::sharded<storage::node> storage_node;
    ss::sharded<kafka::datalake_throttle_manager> manager;
    ss::sharded<shard_local_state> shard_state;
};

ss::future<> mark_producers_on_random_shards(
  ss::sharded<kafka::datalake_throttle_manager>& mgr, int producer_cnt) {
    for (auto i : std::ranges::views::iota(0, producer_cnt)) {
        auto shard = random_generators::get_int<uint32_t>(
          0, ss::smp::count - 1);
        co_await mgr.invoke_on(
          shard, [i](kafka::datalake_throttle_manager& local_mgr) {
              local_mgr.mark_datalake_producer(fmt::format("producer-{}", i));
          });
    }
}

TEST_F(IcebergThrottlingManagerTest, TestThrottlingIcebergEnabledProducer) {
    mark_producers_on_random_shards(manager, 100).get();
    // wait for a while for state to propagate across shards
    ss::sleep(500ms).get();
    ASSERT_EQ(
      manager.local().maybe_throttle_producer("producer-20").get(), 0ms);
    // set the backlog above the threshold on one shards, the throttling should
    // not be applied as no max shares is assigned

    shard_state
      .invoke_on(
        0, [](shard_local_state& state) { state.total_backlog = 200_MiB; })
      .get();

    ASSERT_EQ(
      manager.local().maybe_throttle_producer("producer-20").get(), 0ms);
    // set max shares assigned, the throttling should be applied
    shard_state
      .invoke_on(
        0, [](shard_local_state& state) { state.max_shares_assigned = true; })
      .get();
    wait_for_producer_throttling("producer-20", 5ms).get();
    // wait for some more time, the throttle should increase
    wait_for_producer_throttling("producer-20", 10ms).get();

    // non iceberg producer should not be throttled
    ASSERT_EQ(
      manager.local().maybe_throttle_producer("producer-20-other").get(), 0ms);
    // when translation status does not report any issues, the producer should
    // not be throttled
    shard_state
      .invoke_on(
        0, [](shard_local_state& state) { state.total_backlog = 10_MiB; })
      .get();
    wait_for_no_throttling("producer-20").get();
}

TEST_F(IcebergThrottlingManagerTest, TestProducerEviction) {
    mark_producers_on_random_shards(manager, 100).get();
    // wait for state to propagate
    ss::sleep(500ms).get();
    shard_state
      .invoke_on(
        0,
        [](shard_local_state& state) {
            state.max_shares_assigned = true;
            state.total_backlog = 100_MiB;
        })
      .get();
    wait_for_producer_throttling("producer-20", 3ms).get();
    shard_state
      .invoke_on_all([](shard_local_state& state) {
          state.producer_gc_threshold.update(1s);
      })
      .get();
    // backlog is still above threshold but the producer should be evicted so it
    // should not be throttled
    wait_for_no_throttling("producer-20").get();
}

TEST_F(IcebergThrottlingManagerTest, TestHandlingAnonymousProducers) {
    // mark anonymous producer on random shard
    auto shard = random_generators::get_int<uint32_t>(0, ss::smp::count - 1);
    manager
      .invoke_on(
        shard,
        [](kafka::datalake_throttle_manager& local_mgr) {
            local_mgr.mark_datalake_producer(std::nullopt);
        })
      .get();

    shard_state
      .invoke_on(
        0,
        [](shard_local_state& state) {
            state.max_shares_assigned = true;
            state.total_backlog = 100_MiB;
        })
      .get();
    // check if anonymous producer is throttled
    wait_for_producer_throttling(std::nullopt, 3ms).get();
}
