/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "metrics/instance_info_impl.h"

#include <gtest/gtest.h>

using namespace instance_info;

TEST(instance_info, lookup_known_aws) {
    // m6id.16xlarge: 64 vCPU, 256 GiB, 25 Gbps (== 25e9/8 bytes/sec).
    auto i = lookup(cloud_provider::aws, "m6id.16xlarge");
    ASSERT_TRUE(i.has_value());
    EXPECT_EQ(i->vcpus, 64);
    EXPECT_EQ(i->memory_bytes, 256ULL * 1024 * 1024 * 1024);
    EXPECT_EQ(i->network_bytes_per_sec, 25'000'000'000ULL / 8);
    EXPECT_GT(i->read_iops, 0);
    EXPECT_GT(i->write_iops, 0);
    EXPECT_GT(i->disk_bytes, 0);
}

TEST(instance_info, lookup_known_gcp_and_azure) {
    EXPECT_TRUE(lookup(cloud_provider::gcp, "n2d-standard-16").has_value());
    EXPECT_TRUE(lookup(cloud_provider::azure, "Standard_D16ds_v5").has_value());
}

TEST(instance_info, lookup_unknown_name_misses) {
    EXPECT_FALSE(lookup(cloud_provider::aws, "definitely.not.a.type"));
    EXPECT_FALSE(lookup(cloud_provider::aws, ""));
}

TEST(instance_info, lookup_wrong_provider_misses) {
    // Right name, wrong provider: the (provider, name) key must not match.
    EXPECT_FALSE(lookup(cloud_provider::gcp, "m6id.16xlarge"));
    EXPECT_FALSE(lookup(cloud_provider::azure, "n2d-standard-16"));
}

TEST(instance_info, lookup_by_name_ignores_provider) {
    auto keyed = lookup(cloud_provider::aws, "m6id.16xlarge");
    auto by_name = lookup_by_name("m6id.16xlarge");
    ASSERT_TRUE(keyed.has_value());
    ASSERT_TRUE(by_name.has_value());
    EXPECT_EQ(by_name->vcpus, keyed->vcpus);
    EXPECT_EQ(by_name->memory_bytes, keyed->memory_bytes);
    EXPECT_EQ(by_name->read_iops, keyed->read_iops);
}

TEST(instance_info, lookup_by_name_unknown_misses) {
    EXPECT_FALSE(lookup_by_name("definitely.not.a.type"));
}

TEST(instance_info, table_grouped_by_provider) {
    // Providers form contiguous runs (rows are grouped by provider first).
    auto table = instance_table();
    ASSERT_FALSE(table.empty());
    for (size_t k = 1; k < table.size(); ++k) {
        EXPECT_LE(table[k - 1].provider, table[k].provider);
    }
}

TEST(instance_info, family_ordered_by_size_not_lexicographically) {
    // The whole point of the size sort: *.large precedes *.16xlarge, which
    // lexicographic order ("16xlarge" < "2xlarge" < ... < "large") would not.
    auto sz = [](std::string_view n) {
        return lookup(cloud_provider::aws, n).value().vcpus;
    };
    EXPECT_LT(sz("m6id.large"), sz("m6id.2xlarge"));
    EXPECT_LT(sz("m6id.2xlarge"), sz("m6id.16xlarge"));
}
