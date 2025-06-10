/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/panda_link/table.h"
#include "cluster/panda_link/tests/utils.h"
#include "panda_link/model/types.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

namespace cluster::panda_link {

using ::panda_link::model::connection_config;
using ::panda_link::model::id_t;
using ::panda_link::model::metadata;
using ::panda_link::model::name_t;
using ::panda_link::model::uuid_t;

class panda_link_table_test : public seastar_test {
public:
    virtual ss::future<> SetUpAsync() override { co_await _table.start(); }

    virtual ss::future<> TearDownAsync() override { co_await _table.stop(); }

protected:
    ss::sharded<table> _table;
};

TEST_F_CORO(panda_link_table_test, empty) {
    ASSERT_EQ_CORO(_table.local().size(), 0);
}

TEST_F_CORO(panda_link_table_test, reset_links) {
    table::map_t links;
    links.emplace(
      id_t(1),
      metadata{
        .name = name_t("link1"),
        .uuid = uuid_t(::uuid_t::create()),
        .connection = connection_config{}});
    links.emplace(
      id_t(2),
      metadata{
        .name = name_t("link2"),
        .uuid = uuid_t(::uuid_t::create()),
        .connection = connection_config{}});

    cluster::controller_snapshot snap;
    snap.panda_links.links = links;
    ASSERT_NO_THROW_CORO(
      co_await _table.local().apply_snapshot(model::offset{}, snap));

    ASSERT_EQ_CORO(_table.local().size(), 2);
    cluster::controller_snapshot snap2;
    co_await _table.local().fill_snapshot(snap2);
    EXPECT_EQ(snap2.panda_links.links, links);
}

TEST_F_CORO(panda_link_table_test, reset_links_duplicate_name) {
    table::map_t links;
    links.emplace(
      id_t(1),
      metadata{
        .name = name_t("link1"),
        .uuid = uuid_t(::uuid_t::create()),
        .connection = connection_config{}});
    links.emplace(
      id_t(2),
      metadata{
        .name = name_t("link1"), // Duplicate name
        .uuid = uuid_t(::uuid_t::create()),
        .connection = connection_config{}});

    cluster::controller_snapshot snap;
    snap.panda_links.links = links;
    EXPECT_THROW(
      co_await _table.local().apply_snapshot(model::offset{}, snap),
      std::logic_error);
}

TEST_F_CORO(panda_link_table_test, upsert_success_test) {
    metadata link{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    ASSERT_NO_THROW_CORO(co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, link)));

    ASSERT_EQ_CORO(_table.local().size(), 1);

    auto found_link = _table.local().find_link_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(found_link.has_value());
    EXPECT_EQ(found_link->get(), link);
    auto found_id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(found_id.has_value());
    EXPECT_EQ(found_id.value(), id_t(1));
    found_link = _table.local().find_link_by_id(id_t(1));
    ASSERT_TRUE_CORO(found_link.has_value());
    EXPECT_EQ(found_link->get(), link);

    ASSERT_NO_THROW_CORO(co_await _table.local().apply_update(
      testing::create_remove_command(name_t("link1"))));
    found_link = _table.local().find_link_by_name(name_t("link1"));
    EXPECT_FALSE(found_link.has_value());
    found_link = _table.local().find_link_by_id(id_t(1));
    EXPECT_FALSE(found_link.has_value());
    found_id = _table.local().find_id_by_name(name_t("link1"));
    EXPECT_FALSE(found_id.has_value());
}

TEST_F_CORO(panda_link_table_test, upsert_update) {
    auto first_uuid = uuid_t(::uuid_t::create());
    auto second_uuid = uuid_t(::uuid_t::create());
    metadata link{
      .name = name_t("link1"),
      .uuid = first_uuid,
      .connection = connection_config{}};
    metadata updated_link{
      .name = name_t("link1"),
      .uuid = second_uuid,
      .connection = connection_config{}};

    ASSERT_NO_THROW_CORO(co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, link)));
    ASSERT_EQ_CORO(_table.local().size(), 1);
    auto found_link = _table.local().find_link_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(found_link.has_value());
    EXPECT_EQ(found_link->get(), link);

    ASSERT_NO_THROW_CORO(co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{2}, updated_link)));
    EXPECT_EQ(_table.local().size(), 1);
    found_link = _table.local().find_link_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(found_link.has_value());
    EXPECT_EQ(found_link->get(), updated_link);
}

TEST_F_CORO(panda_link_table_test, upsert_duplicate_name) {
    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto ec = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, link1));
    ASSERT_EQ_CORO(ec, cluster::errc::success);
    ec = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{2}, link1));
    ASSERT_EQ_CORO(ec, cluster::errc::success);
}

TEST_F_CORO(panda_link_table_test, remove_non_existent_link) {
    EXPECT_EQ(_table.local().size(), 0);
    EXPECT_NO_THROW(co_await _table.local().apply_update(
      testing::create_remove_command(name_t("nonexistent"))));
    EXPECT_EQ(_table.local().size(), 0);
}

TEST_F_CORO(panda_link_table_test, validate_batch_applicable) {
    auto upsert = testing::create_upsert_command(
      model::offset{1},
      metadata{
        .name = name_t("link1"),
        .uuid = uuid_t(::uuid_t::create()),
        .connection = connection_config{}});
    auto remove = testing::create_remove_command(name_t("link1"));
    EXPECT_TRUE(_table.local().is_batch_applicable(upsert));
    EXPECT_TRUE(_table.local().is_batch_applicable(remove));
    cluster::feature_update_license_update_cmd feature_update_cmd(
      cluster::feature_update_license_update_cmd_data{}, 0);
    auto batch = cluster::serde_serialize_cmd(std::move(feature_update_cmd));
    EXPECT_FALSE(_table.local().is_batch_applicable(batch));
    return ss::now();
}
} // namespace cluster::panda_link
