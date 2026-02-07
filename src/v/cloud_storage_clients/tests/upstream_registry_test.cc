/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/upstream_registry.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace cloud_storage_clients;
using namespace testing;

bucket_name_parts
make_bucket(std::vector<std::pair<ss::sstring, ss::sstring>> params = {}) {
    return {
      .name = plain_bucket_name{"test-bucket"}, .params = std::move(params)};
}

} // namespace

TEST(MakeUpstreamKey, EmptyParams) {
    auto key = make_upstream_key(s3_configuration{}, make_bucket());
    EXPECT_TRUE(key.region().empty());
    EXPECT_TRUE(key.endpoint().empty());
    EXPECT_FALSE(key.server_addr.has_value());
}

TEST(MakeUpstreamKey, ParsesParamsPathStyle) {
    s3_configuration cfg;
    cfg.url_style = s3_url_style::path;
    cfg.server_addr = net::unresolved_address("default.example.com", 443);

    auto key = make_upstream_key(
      cfg,
      make_bucket({{"region", "us-west-2"}, {"endpoint", "s3.example.com"}}));

    EXPECT_EQ(key.region(), "us-west-2");
    EXPECT_EQ(key.endpoint(), "s3.example.com");
    ASSERT_TRUE(key.server_addr.has_value());
    EXPECT_EQ(key.server_addr->host(), "s3.example.com");
    EXPECT_EQ(key.server_addr->port(), 443);
}

TEST(MakeUpstreamKey, ParsesParamsVirtualHostStyle) {
    s3_configuration cfg;
    cfg.url_style = s3_url_style::virtual_host;
    cfg.server_addr = net::unresolved_address("default.example.com", 443);

    auto key = make_upstream_key(
      cfg,
      make_bucket({{"region", "us-west-2"}, {"endpoint", "s3.example.com"}}));

    EXPECT_EQ(key.region(), "us-west-2");
    EXPECT_EQ(key.endpoint(), "s3.example.com");
    ASSERT_TRUE(key.server_addr.has_value());
    EXPECT_EQ(key.server_addr->host(), "test-bucket.s3.example.com");
    EXPECT_EQ(key.server_addr->port(), 443);
}

TEST(MakeUpstreamKey, RejectsParamsForABS) {
    EXPECT_THROW(
      make_upstream_key(
        abs_configuration{}, make_bucket({{"region", "us-west-2"}})),
      std::invalid_argument);
}

TEST(MakeUpstreamKey, RejectsParamsForGCS) {
    s3_configuration gcs_cfg;
    gcs_cfg.is_gcs = true;
    EXPECT_THROW(
      make_upstream_key(gcs_cfg, make_bucket({{"region", "us-west-2"}})),
      std::invalid_argument);
}

TEST(MakeUpstreamKey, RejectsUnknownParam) {
    s3_configuration cfg;
    cfg.url_style = s3_url_style::path;
    EXPECT_THROW(
      make_upstream_key(cfg, make_bucket({{"foo", "bar"}})),
      std::invalid_argument);
}

TEST(MakeUpstreamKey, RejectsParamsWithoutUrlStyle) {
    EXPECT_THAT(
      [] {
          make_upstream_key(
            s3_configuration{}, make_bucket({{"region", "us-west-2"}}));
      },
      ThrowsMessage<std::invalid_argument>(StrEq(
        "Cannot specify bucket params when cloud storage url style is not set "
        "explicitly")));
}
