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

#include "cloud_instance_metadata/aws_imds.h"
#include "cloud_instance_metadata/azure_imds.h"
#include "cloud_instance_metadata/gcp_metadata.h"

#include <boost/beast/http/verb.hpp>
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

// These are golden tests: consumers (cloud_roles credential refresh, the
// instance-type detector) rely on these builders producing exactly the
// headers, in exactly the order, that they historically built inline, so the
// bytes on the wire are unchanged.

namespace bh = boost::beast::http;
namespace imds = cloud_instance_metadata::aws_imds;
namespace gcp = cloud_instance_metadata::gcp_metadata;
namespace azure = cloud_instance_metadata::azure_imds;

namespace {

using field_list = std::vector<std::pair<std::string, std::string>>;

field_list fields_of(const bh::request_header<>& req) {
    field_list out;
    for (const auto& f : req) {
        out.emplace_back(f.name_string(), f.value());
    }
    return out;
}

} // namespace

TEST(aws_imds, token_request) {
    auto req = imds::token_request(imds::host, std::chrono::seconds{21600});
    EXPECT_EQ(req.method(), bh::verb::put);
    EXPECT_EQ(req.target(), "/latest/api/token");
    EXPECT_EQ(
      fields_of(req),
      (field_list{
        {"Host", "169.254.169.254"},
        {"X-aws-ec2-metadata-token-ttl-seconds", "21600"}}));
}

TEST(aws_imds, get_with_token) {
    auto req = imds::get("imds.test", imds::instance_type_path, "tok-123");
    EXPECT_EQ(req.method(), bh::verb::get);
    EXPECT_EQ(req.target(), "/latest/meta-data/instance-type");
    EXPECT_EQ(
      fields_of(req),
      (field_list{
        {"Host", "imds.test"}, {"X-aws-ec2-metadata-token", "tok-123"}}));
}

TEST(aws_imds, get_without_token) {
    auto req = imds::get(
      imds::host, "/latest/meta-data/iam/security-credentials/");
    EXPECT_EQ(req.method(), bh::verb::get);
    EXPECT_EQ(req.target(), "/latest/meta-data/iam/security-credentials/");
    EXPECT_EQ(fields_of(req), (field_list{{"Host", "169.254.169.254"}}));
}

TEST(gcp_metadata, get) {
    auto req = gcp::get(
      "/computeMetadata/v1/instance/service-accounts/default/token");
    EXPECT_EQ(req.method(), bh::verb::get);
    EXPECT_EQ(
      req.target(),
      "/computeMetadata/v1/instance/service-accounts/default/token");
    // No Host header: matches what the gcp credentials consumer always sent.
    EXPECT_EQ(fields_of(req), (field_list{{"Metadata-Flavor", "Google"}}));
}

TEST(azure_imds, get) {
    auto req = azure::get(
      "/metadata/identity/oauth2/token?api-version=2018-02-01");
    EXPECT_EQ(req.method(), bh::verb::get);
    EXPECT_EQ(
      req.target(), "/metadata/identity/oauth2/token?api-version=2018-02-01");
    // No Host header: matches what the azure_vm credentials consumer always
    // sent.
    EXPECT_EQ(fields_of(req), (field_list{{"Metadata", "true"}}));
}
