/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cloud_metadata/key_utils.h"

#include <gtest/gtest.h>

using namespace cluster::cloud_metadata;

TEST(key_utils, cluster_name_ref_for_uuid_prefix_key) {
    ASSERT_EQ(
      cluster_name_ref_for_uuid_prefix_key("my-valid_cluster"),
      "cluster_name/my-valid_cluster/uuid/");
}

TEST(key_utils, cluster_name_ref_for_uuid_key) {
    ASSERT_EQ(
      cluster_name_ref_for_uuid_key(
        "my-valid_cluster",
        model::cluster_uuid(
          uuid_t::from_string("84b42020-e209-4c6f-a63f-6419441fe012"))),
      "cluster_name/my-valid_cluster/uuid/"
      "84b42020-e209-4c6f-a63f-6419441fe012");
}

TEST(key_utils, parse_cluster_name_ref_for_uuid_key) {
    auto parsed = parse_cluster_name_ref_for_uuid_key(
      "cluster_name/my-Valid_Cluster/uuid/"
      "84b42020-e209-4c6f-a63f-6419441fe012");
    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(std::get<0>(parsed.value()), "my-Valid_Cluster");
    ASSERT_EQ(
      std::get<1>(parsed.value()),
      model::cluster_uuid(
        uuid_t::from_string("84b42020-e209-4c6f-a63f-6419441fe012")));
}

TEST(key_utils, parse_cluster_name_ref_for_uuid_key_invalid) {
    ASSERT_FALSE(parse_cluster_name_ref_for_uuid_key("wat").has_value())
      << "Should not parse arbitrary keys";

    ASSERT_FALSE(parse_cluster_name_ref_for_uuid_key(
                   "cluster_name/my-valid_cluster/uuid/"
                   "yolo-84b42020-e209-4c6f-a63f-6419441fe012")
                   .has_value())
      << "Should not parse invalid UUIDs";

    ASSERT_FALSE(parse_cluster_name_ref_for_uuid_key(
                   "cluster_name/my-!nval!d_cluster/uuid/"
                   "84b42020-e209-4c6f-a63f-6419441fe012")
                   .has_value())
      << "Should not parse invalid cluster names";

    ASSERT_FALSE(parse_cluster_name_ref_for_uuid_key(
                   "cluster_name/my-valid_cluster/uuid/"
                   "84b42020-e209-4c6f-a63f-6419441fe012/extra")
                   .has_value())
      << "Should not parse keys with extra characters";
}
