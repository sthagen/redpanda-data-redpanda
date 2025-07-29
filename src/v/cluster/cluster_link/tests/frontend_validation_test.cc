/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/frontend.h"
#include "cluster/cluster_link/table.h"
#include "cluster/cluster_link/tests/utils.h"
#include "test_utils/test.h"
#include "utils/unresolved_address.h"

#include <gtest/gtest.h>

namespace cluster::cluster_link {

using ::cluster_link::model::add_mirror_topic_cmd;
using ::cluster_link::model::connection_config;
using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::mirror_topic_state;
using ::cluster_link::model::name_t;
using ::cluster_link::model::tls_file_path;
using ::cluster_link::model::tls_value;
using ::cluster_link::model::update_mirror_topic_state_cmd;
using ::cluster_link::model::uuid_t;

constexpr size_t max_links = 1;

class frontend_validation_test : public seastar_test {
public:
    ss::sharded<table> _table;

    std::unique_ptr<frontend::validator> _validator{nullptr};

    ss::future<> SetUpAsync() override {
        co_await _table.start();
        _validator = std::make_unique<frontend::validator>(
          &_table.local(), max_links);
    }
    ss::future<> TearDownAsync() override {
        _validator.reset(nullptr);
        co_await _table.stop();
    }

    ss::future<cluster::cluster_link::errc> upsert_cluster_link(metadata m) {
        cluster::cluster_link_upsert_cmd cmd{0, m.copy()};
        auto ec = _validator->validate_mutation(std::move(cmd));
        if (ec == cluster::cluster_link::errc::success) {
            auto existing = _table.local().find_id_by_name(m.name);
            auto id = existing.value_or(++_latest_id);
            auto err = co_await _table.local().apply_update(
              testing::create_upsert_command(
                model::offset{id()}, std::move(m)));
            vassert(!err, "Failed to upsert link: {}", err.message());
        }

        co_return ec;
    }

    ss::future<cluster::cluster_link::errc> delete_cluster_link(name_t m) {
        cluster::cluster_link_remove_cmd cmd{std::move(m), 0};
        auto ec = _validator->validate_mutation(cmd);
        if (ec == cluster::cluster_link::errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_remove_command(cmd.key));
            vassert(!err, "Failed to remove link: {}", err.message());
        }
        co_return ec;
    }

    ss::future<cluster::cluster_link::errc>
    add_mirror_topic(id_t id, add_mirror_topic_cmd cmd) {
        cluster::cluster_link_add_mirror_topic_cmd add_cmd{id, std::move(cmd)};
        auto ec = _validator->validate_mutation(add_cmd);
        if (ec == errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_add_mirror_topic_command(
                add_cmd.key, std::move(add_cmd.value)));
            vassert(!err, "Failed to add mirror topic: {}", err.message());
        }
        co_return ec;
    }

    ss::future<cluster::cluster_link::errc>
    update_mirror_topic_state(id_t id, update_mirror_topic_state_cmd cmd) {
        cluster::cluster_link_update_mirror_topic_state_cmd update_cmd{
          id, std::move(cmd)};
        auto ec = _validator->validate_mutation(update_cmd);
        if (ec == errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_update_mirror_topic_state_command(
                update_cmd.key, std::move(update_cmd.value)));
            vassert(
              !err, "Failed to update mirror topic state: {}", err.message());
        }
        co_return ec;
    }

    id_t _latest_id{0};
};

TEST_F_CORO(frontend_validation_test, successful_upsert) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, too_many_links) {
    for (size_t i = 0; i < max_links; ++i) {
        metadata m{
          .name = name_t(fmt::format("link{}", i + 1)),
          .uuid = uuid_t(::uuid_t::create()),
          .connection = connection_config{
            .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m)),
          cluster::cluster_link::errc::success);
    }
    metadata m2{
      .name = name_t("toomany"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m2)),
      cluster::cluster_link::errc::limit_exceeded);
}

TEST_F_CORO(frontend_validation_test, no_bootstrap_servers) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::invalid_create);
}

TEST_F_CORO(frontend_validation_test, name_too_long) {
    metadata m{
      .name = name_t(std::string(129, 'a')),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::invalid_create);
}

TEST_F_CORO(frontend_validation_test, name_empty) {
    metadata m{
      .name = name_t(""),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::invalid_create);
}

TEST_F_CORO(frontend_validation_test, remote_non_existent) {
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("nonexistent")),
      cluster::cluster_link::errc::does_not_exist);
}

TEST_F_CORO(frontend_validation_test, remove_existing) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::success);
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1")),
      cluster::cluster_link::errc::success);
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1")),
      cluster::cluster_link::errc::does_not_exist);
}

TEST_F_CORO(frontend_validation_test, update_existing_bad_uuid) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::success);
    metadata mupdate{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(mupdate)),
      cluster::cluster_link::errc::invalid_update);
}

TEST_F_CORO(frontend_validation_test, update_existing_good_uuid) {
    auto link_uuid = uuid_t(::uuid_t::create());
    metadata m{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::success);
    metadata mupdate{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost1", 9092}}}};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(mupdate)),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, update_no_bootstrap_servers) {
    auto link_uuid = uuid_t(::uuid_t::create());
    metadata m{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::success);
    metadata mupdate{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{}};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(mupdate)),
      cluster::cluster_link::errc::invalid_update);
}

TEST_F_CORO(frontend_validation_test, invalid_utf8_in_name) {
    metadata m{
      .name = name_t("\xFF\xFF\xFF"), // Invalid UTF-8
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::invalid_create);
}

TEST_F_CORO(frontend_validation_test, control_character_in_name) {
    metadata m{
      .name = name_t("link1\x0d"), // Contains a control character
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::invalid_create);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_missing_key) {
    metadata m{
      .name = name_t("link1\x0d"), // Contains a control character
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
        .cert = tls_value("bah")}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::invalid_create);
}

TEST_F_CORO(
  frontend_validation_test, add_mirror_topic_key_cert_types_different) {
    metadata m{
      .name = name_t("link1\x0d"), // Contains a control character
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
        .cert = tls_value("bah"),
        .key = tls_file_path("key.pem")}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::invalid_create);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_success) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = model::topic("mirror-topic"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_state::active, model::topic("mirror-topic"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)), errc::success);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_invalid_name) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = model::topic("\xFF\xFF\xFF"), // Invalid UTF-8
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_state::active, model::topic("\xFF\xFF\xFF"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)),
      errc::mirror_topic_name_invalid);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_no_link) {
    add_mirror_topic_cmd cmd{
      .topic = model::topic("mirror-topic"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_state::active, model::topic("mirror-topic"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id_t{5}, std::move(cmd)), errc::does_not_exist);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_already_mirrored) {
    model::topic test_topic("mirror-link1");
    mirror_topic_state mirror_state = mirror_topic_state::active;
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    m.state.set_mirror_topics(
      {{test_topic,
        testing::create_mirror_topic_metadata(mirror_state, test_topic)}});
    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = test_topic,
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_state::active, test_topic)};

    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)),
      errc::topic_already_being_mirrored);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_mirrored_by_other_link) {
    model::topic test_topic("mirror-link1");
    mirror_topic_state mirror_state = mirror_topic_state::active;
    metadata m1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    m1.state.set_mirror_topics(
      {{test_topic,
        testing::create_mirror_topic_metadata(mirror_state, test_topic)}});

    metadata m2{
      .name = name_t("link2"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{1}, std::move(m1))),
      errc::success);

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{2}, std::move(m2))),
      errc::success);

    add_mirror_topic_cmd cmd{
      .topic = test_topic,
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_state::active, test_topic)};

    EXPECT_EQ(
      co_await add_mirror_topic(id_t{2}, std::move(cmd)),
      errc::topic_being_mirrored_by_other_link);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_state_success) {
    model::topic test_topic("mirror-link1");
    mirror_topic_state mirror_state = mirror_topic_state::active;
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    m.state.set_mirror_topics(
      {{test_topic,
        testing::create_mirror_topic_metadata(mirror_state, test_topic)}});
    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    update_mirror_topic_state_cmd update_cmd{
      .topic = test_topic, .state = mirror_topic_state::paused};
    EXPECT_EQ(
      co_await update_mirror_topic_state(id.value(), std::move(update_cmd)),
      errc::success);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_state_invalid_name) {
    model::topic test_topic("mirror-link1");
    mirror_topic_state mirror_state = mirror_topic_state::active;
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    m.state.set_mirror_topics(
      {{test_topic,
        testing::create_mirror_topic_metadata(mirror_state, test_topic)}});
    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    update_mirror_topic_state_cmd update_cmd{
      .topic = model::topic("\xFF\xFF\xFF"), // Invalid UTF-8
      .state = mirror_topic_state::paused};
    EXPECT_EQ(
      co_await update_mirror_topic_state(id.value(), std::move(update_cmd)),
      errc::mirror_topic_name_invalid);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_non_existant_link) {
    update_mirror_topic_state_cmd update_cmd{
      .topic = model::topic("test-topic"), .state = mirror_topic_state::paused};
    EXPECT_EQ(
      co_await update_mirror_topic_state(id_t{5}, std::move(update_cmd)),
      errc::does_not_exist);
}

TEST_F_CORO(
  frontend_validation_test, update_mirror_topic_mirror_topic_does_not_exist) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    update_mirror_topic_state_cmd update_cmd{
      .topic = model::topic("test-topic"), .state = mirror_topic_state::paused};
    EXPECT_EQ(
      co_await update_mirror_topic_state(id.value(), std::move(update_cmd)),
      errc::topic_not_being_mirrored);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_mirrored_by_other) {
    model::topic test_topic("mirror-link1");
    mirror_topic_state mirror_state = mirror_topic_state::active;
    metadata m1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    m1.state.set_mirror_topics(
      {{test_topic,
        testing::create_mirror_topic_metadata(mirror_state, test_topic)}});

    metadata m2{
      .name = name_t("link2"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{1}, std::move(m1))),
      errc::success);

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{2}, std::move(m2))),
      errc::success);

    update_mirror_topic_state_cmd update_cmd{
      .topic = test_topic, .state = mirror_topic_state::paused};

    EXPECT_EQ(
      co_await update_mirror_topic_state(id_t{2}, std::move(update_cmd)),
      errc::topic_being_mirrored_by_other_link);
}

} // namespace cluster::cluster_link
