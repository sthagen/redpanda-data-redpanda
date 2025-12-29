/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/object_utils.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#define UUID_REGEX                                                             \
    "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

#define PREFIX_REGEX "[0-9]{3}"

TEST(ObjectPathFactory, LevelZeroPathFormat) {
    auto path = cloud_topics::object_path_factory::level_zero_path(
      cloud_topics::object_id::create(cloud_topics::cluster_epoch{42}));
    EXPECT_THAT(
      path().string(),
      ::testing::MatchesRegex(
        "^level_zero/data/" PREFIX_REGEX "/000000000000000042/" UUID_REGEX
        "$"));
}

TEST(ObjectPathFactory, LevelZeroDataDir) {
    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_data_dir(),
      cloud_storage_clients::object_key("level_zero/data/"));
}

TEST(ObjectPathFactory, LevelZeroParseEpoch) {
    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/000/000000000000010042/"),
      cloud_topics::cluster_epoch(10042));

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/000/000000000000010042/asdfalksjdflkjsdflkj"),
      cloud_topics::cluster_epoch(10042));

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_asdf_zero/data/000/000000000000010042/asdfasdf")
        .error(),
      "L0 object name missing prefix: "
      "level_asdf_zero/data/000/000000000000010042/asdfasdf");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/000/0000000000010042/")
        .error(),
      "L0 object name is too short: level_zero/data/000/0000000000010042/");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/00/")
        .error(),
      "L0 object name is too short: level_zero/data/00/");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/000/00000X0000000010042/asdfasdf")
        .error(),
      "L0 object name has invalid epoch: "
      "level_zero/data/000/00000X0000000010042/asdfasdf");
}

TEST(ObjectPathFactory, LevelZeroParsePrefix) {
    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_zero/data/123/"),
      123);

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_zero/data/123/000000000000010042/"),
      123);

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_asdf_zero/data/000/000000000000010042/asdfasdf")
        .error(),
      "L0 object name missing prefix: "
      "level_asdf_zero/data/000/000000000000010042/asdfasdf");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_zero/data/00/")
        .error(),
      "L0 object name is too short: level_zero/data/00/");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_zero/data/0X0/0000000000000010042/asdfasdf")
        .error(),
      "L0 object name has invalid prefix: "
      "level_zero/data/0X0/0000000000000010042/asdfasdf");
}
