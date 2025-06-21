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

#include "cluster/cluster_link/table.h"
#include "cluster/cluster_link/tests/utils.h"
#include "cluster_link/link.h"
#include "cluster_link/manager.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace cluster_link {

using ::cluster::cluster_link::table;

class link_test;
namespace {
class test_link_registry : public link_registry {
public:
    explicit test_link_registry(table* table)
      : _table(table) {}

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_id(model::id_t id) const override {
        return _table->find_link_by_id(id);
    }

    std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t& name) const override {
        return _table->find_link_by_name(name);
    }

    chunked_vector<model::id_t> get_all_link_ids() const override {
        return _table->get_all_link_ids();
    }

private:
    table* _table;
};

class test_link : public link {
public:
    test_link(link_test* link_test, model::metadata metadata);

    ss::future<> start() override;
    ss::future<> stop() override;

private:
    link_test* _link_test;
};

class test_link_factory : public link_factory {
public:
    explicit test_link_factory(link_test* link_test)
      : _link_test(link_test) {}
    std::unique_ptr<link> create_link(model::metadata metadata) override {
        return std::make_unique<test_link>(_link_test, std::move(metadata));
    }

private:
    link_test* _link_test;
};
} // namespace

class link_test : public seastar_test {
public:
    virtual ss::future<> SetUpAsync() override {
        co_await _table.start();
        _manager = std::make_unique<manager>(
          ::model::node_id(0),
          std::make_unique<test_link_registry>(&_table.local()),
          std::make_unique<test_link_factory>(this));
    }

    virtual ss::future<> TearDownAsync() override {
        _manager.reset(nullptr);
        co_await _table.stop();
    }

    ss::future<> upsert_link(model::id_t id, model::metadata metadata) {
        co_await _table.local().apply_update(
          ::cluster::cluster_link::testing::create_upsert_command(
            ::model::offset{id()}, metadata));
        _manager->on_link_change(id);
    }

    ss::future<> remove_link(const model::name_t& name) {
        auto id = _table.local().find_id_by_name(name);
        co_await _table.local().apply_update(
          ::cluster::cluster_link::testing::create_remove_command(name));
        if (id.has_value()) {
            _manager->on_link_change(id.value());
        }
    }

    void add_link_to_list(uuid_t id, test_link* link) {
        _links.emplace(id, link);
        run_callbacks(id);
    }

    void remove_link(uuid_t id) {
        _links.erase(id);
        run_callbacks(id);
    }

    void run_callbacks(uuid_t id) {
        for (const auto& [_, cb] : _callbacks) {
            cb(id);
        }
    }

    using notification_id = named_type<size_t, struct test_notification_tag>;
    using notification_callback = ss::noncopyable_function<void(uuid_t)>;

    notification_id
    register_callback(ss::noncopyable_function<void(uuid_t)> cb) {
        auto it = _callbacks.insert({++_latest_id, std::move(cb)});
        vassert(it.second, "Invalid duplicate in callbacks");
        return _latest_id;
    }

    void unregister_callback(notification_id id) { _callbacks.erase(id); }

protected:
    ss::sharded<table> _table;
    std::unique_ptr<manager> _manager;
    absl::flat_hash_map<uuid_t, test_link*> _links;
    absl::flat_hash_map<notification_id, ss::noncopyable_function<void(uuid_t)>>
      _callbacks;
    notification_id _latest_id{0};
};

class link_test_manager_started : public link_test {
public:
    ss::future<> SetUpAsync() override {
        co_await link_test::SetUpAsync();
        co_await _manager->start();
    }

    ss::future<> TearDownAsync() override {
        co_await _manager->stop();
        co_await link_test::TearDownAsync();
    }
};

namespace {
test_link::test_link(link_test* link_test, model::metadata metadata)
  : link(std::move(metadata))
  , _link_test(link_test) {}

ss::future<> test_link::start() {
    co_await link::start();
    _link_test->add_link_to_list(config().uuid, this);
}

ss::future<> test_link::stop() {
    _link_test->remove_link(config().uuid);
    co_await link::stop();
}
} // namespace

TEST_F_CORO(link_test, start_with_table_entries) {
    auto link_uuid = model::uuid_t(::uuid_t::create());
    model::metadata link{
      .name = model::name_t("link1"),
      .uuid = link_uuid,
      .connection = model::connection_config{}};
    model::id_t link_id(1);
    ss::condition_variable cv;

    auto callback_id = register_callback([&cv](uuid_t) { cv.signal(); });
    auto remove_callback = ss::defer(
      [this, callback_id] { unregister_callback(callback_id); });

    co_await upsert_link(link_id, link);
    co_await _manager->start();
    ASSERT_NO_THROW_CORO(co_await cv.wait(5s))
      << "Timed out waiting for link creation";
    auto it = _links.find(link_uuid);
    ASSERT_NE_CORO(it, _links.end())
      << "Unable to find link with UUID: " << link_uuid;
    EXPECT_EQ(it->second->config(), link);
    co_await _manager->stop();
}

TEST_F_CORO(link_test_manager_started, test_create_link_and_update) {
    auto link_uuid = model::uuid_t(::uuid_t::create());
    model::metadata link{
      .name = model::name_t("link1"),
      .uuid = link_uuid,
      .connection = model::connection_config{}};
    model::id_t link_id(1);
    ss::condition_variable cv;

    auto callback_id = register_callback([&cv](uuid_t) { cv.signal(); });
    auto remove_callback = ss::defer(
      [this, callback_id] { unregister_callback(callback_id); });

    co_await upsert_link(link_id, link);
    ASSERT_NO_THROW_CORO(co_await cv.wait(5s))
      << "Timed out waiting for link creation";
    auto it = _links.find(link_uuid);
    ASSERT_NE_CORO(it, _links.end())
      << "Unable to find link with UUID: " << link_uuid;
    EXPECT_EQ(it->second->config(), link);

    model::metadata updated_link{
      .name = model::name_t("link1"),
      .uuid = link_uuid,
      .connection = model::connection_config{
        .bootstrap_servers{net::unresolved_address{"localhost", 9092}}}};
    co_await upsert_link(link_id, updated_link);

    it = _links.find(link_uuid);
    ASSERT_NE_CORO(it, _links.end())
      << "Unable to find link with UUID: " << link_uuid;
    for (auto i = 0; i < 5; ++i) {
        if (it->second->config() == updated_link) {
            break;
        }
        co_await ss::sleep(100ms);
    }
    ASSERT_EQ_CORO(it->second->config(), updated_link)
      << "Link configuration did not update after 5 attempts";
}

TEST_F_CORO(link_test_manager_started, test_remove_link) {
    auto link_uuid = model::uuid_t(::uuid_t::create());
    model::metadata link{
      .name = model::name_t("link1"),
      .uuid = link_uuid,
      .connection = model::connection_config{}};
    model::id_t link_id(1);
    ss::condition_variable cv;

    auto callback_id = register_callback([&cv](uuid_t) { cv.signal(); });
    auto remove_callback = ss::defer(
      [this, callback_id] { unregister_callback(callback_id); });

    co_await upsert_link(link_id, link);
    ASSERT_NO_THROW_CORO(co_await cv.wait(5s))
      << "Timed out waiting for link creation";
    auto it = _links.find(link_uuid);
    ASSERT_NE_CORO(it, _links.end())
      << "Unable to find link with UUID: " << link_uuid;

    co_await remove_link(model::name_t("link1"));
    ASSERT_NO_THROW_CORO(co_await cv.wait(5s))
      << "Timed out waiting for link deletion";
    it = _links.find(link_uuid);
    EXPECT_EQ(it, _links.end())
      << "Link with UUID: " << link_uuid << " was not removed";
}

TEST_F_CORO(link_test_manager_started, test_remove_non_existant_link) {
    _manager->on_link_change(model::id_t(1));
    return ss::now();
}
} // namespace cluster_link
