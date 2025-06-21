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

using ::cluster_link::model::connection_config;
using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::name_t;
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

    ss::future<cluster::errc> upsert_cluster_link(metadata m) {
        cluster::cluster_link_upsert_cmd cmd{0, std::move(m)};
        auto ec = _validator->validate_mutation(cmd);
        if (ec == cluster::errc::success) {
            auto existing = _table.local().find_id_by_name(cmd.value.name);
            auto id = existing.value_or(++_latest_id);
            co_await _table.local().apply_update(
              testing::create_upsert_command(model::offset{id()}, cmd.value));
        }

        co_return ec;
    }

    ss::future<cluster::errc> delete_cluster_link(name_t m) {
        cluster::cluster_link_remove_cmd cmd{std::move(m), 0};
        auto ec = _validator->validate_mutation(cmd);
        if (ec == cluster::errc::success) {
            co_await _table.local().apply_update(
              testing::create_remove_command(cmd.key));
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
      co_await upsert_cluster_link(std::move(m)), cluster::errc::success);
}

TEST_F_CORO(frontend_validation_test, too_many_links) {
    for (size_t i = 0; i < max_links; ++i) {
        metadata m{
          .name = name_t(fmt::format("link{}", i + 1)),
          .uuid = uuid_t(::uuid_t::create()),
          .connection = connection_config{
            .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m)), cluster::errc::success);
    }
    metadata m2{
      .name = name_t("toomany"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m2)),
      cluster::errc::cluster_link_limit_exceeded);
}

TEST_F_CORO(frontend_validation_test, no_bootstrap_servers) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::errc::cluster_link_invalid_create);
}

TEST_F_CORO(frontend_validation_test, name_too_long) {
    metadata m{
      .name = name_t(std::string(129, 'a')),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::errc::cluster_link_invalid_create);
}

TEST_F_CORO(frontend_validation_test, name_empty) {
    metadata m{
      .name = name_t(""),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::errc::cluster_link_invalid_create);
}

TEST_F_CORO(frontend_validation_test, remote_non_existent) {
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("nonexistent")),
      cluster::errc::cluster_link_does_not_exist);
}

TEST_F_CORO(frontend_validation_test, remove_existing) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)), cluster::errc::success);
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1")), cluster::errc::success);
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1")),
      cluster::errc::cluster_link_does_not_exist);
}

TEST_F_CORO(frontend_validation_test, update_existing_bad_uuid) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)), cluster::errc::success);
    metadata mupdate{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(mupdate)),
      cluster::errc::cluster_link_invalid_update);
}

TEST_F_CORO(frontend_validation_test, update_existing_good_uuid) {
    auto link_uuid = uuid_t(::uuid_t::create());
    metadata m{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)), cluster::errc::success);
    metadata mupdate{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost1", 9092}}}};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(mupdate)), cluster::errc::success);
}

TEST_F_CORO(frontend_validation_test, update_no_bootstrap_servers) {
    auto link_uuid = uuid_t(::uuid_t::create());
    metadata m{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)), cluster::errc::success);
    metadata mupdate{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{}};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(mupdate)),
      cluster::errc::cluster_link_invalid_update);
}

TEST_F_CORO(frontend_validation_test, invalid_utf8_in_name) {
    metadata m{
      .name = name_t("\xFF\xFF\xFF"), // Invalid UTF-8
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::errc::cluster_link_invalid_create);
}

TEST_F_CORO(frontend_validation_test, control_character_in_name) {
    metadata m{
      .name = name_t("link1\x0d"), // Contains a control character
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::errc::cluster_link_invalid_create);
}

} // namespace cluster::cluster_link
