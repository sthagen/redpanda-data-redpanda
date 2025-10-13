/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/configuration.h"
#include "config/configuration.h"

#include <gtest/gtest.h>

TEST(BackendDetectionTest, BackendFromUrl) {
    auto cfg = cloud_storage_clients::s3_configuration{};
    cfg.uri = cloud_storage_clients::access_point_uri{"storage.googleapis.com"};
    auto inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::config_file);
    EXPECT_EQ(inferred, model::cloud_storage_backend::google_s3_compat);

    cfg.uri = cloud_storage_clients::access_point_uri{
      "gb-lon-1.linodeobjects.com"};
    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::config_file);
    EXPECT_EQ(inferred, model::cloud_storage_backend::linode_s3_compat);

    cfg.uri = cloud_storage_clients::access_point_uri{"minio-s3"};
    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::config_file);
    EXPECT_EQ(inferred, model::cloud_storage_backend::minio);
}

TEST(BackendDetectionTest, BackendFromCredSource) {
    auto cfg = cloud_storage_clients::s3_configuration{};
    auto inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::aws_instance_metadata);
    EXPECT_EQ(inferred, model::cloud_storage_backend::aws);

    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::sts);
    EXPECT_EQ(inferred, model::cloud_storage_backend::aws);

    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::gcp_instance_metadata);
    EXPECT_EQ(inferred, model::cloud_storage_backend::google_s3_compat);

    inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::config_file);
    EXPECT_EQ(inferred, model::cloud_storage_backend::unknown);
}

TEST(BackendDetectionTest, BackendWhenUsingAzure) {
    auto cfg = cloud_storage_clients::abs_configuration{};
    auto inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::aws_instance_metadata);
    EXPECT_EQ(inferred, model::cloud_storage_backend::azure);
}

TEST(BackendDetectionTest, BackendOverride) {
    config::shard_local_cfg().cloud_storage_backend.set_value(
      model::cloud_storage_backend{
        model::cloud_storage_backend::google_s3_compat});
    auto cfg = cloud_storage_clients::abs_configuration{};
    auto inferred = cloud_storage_clients::infer_backend_from_configuration(
      cfg, model::cloud_credentials_source::aws_instance_metadata);
    EXPECT_EQ(inferred, model::cloud_storage_backend::google_s3_compat);
}
