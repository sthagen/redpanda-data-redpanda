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

#include "bytes/iobuf.h"
#include "http/client.h"
#include "metrics/instance_type_detector.h"
#include "src/v/metrics/instance_type_detector_impl.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <gtest/gtest.h>

#include <cstdlib>

using namespace instance_info;
namespace bh = boost::beast::http;

namespace {

iobuf make_body(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

// A fake IMDS endpoint: answers the PUT token request and the GET
// instance-type request with canned status/body, and records whether the GET
// carried the session token header.
class fake_imds_client : public http::abstract_client {
public:
    bh::status token_status{bh::status::ok};
    ss::sstring token_body{"AQAE-token"};
    bh::status meta_status{bh::status::ok};
    ss::sstring meta_body{"m6id.4xlarge"};
    bool get_had_token_header{false};

    ss::future<http::downloaded_response> request_and_collect_response(
      bh::request_header<>&& req,
      std::optional<iobuf> /*payload*/,
      ss::lowres_clock::duration /*timeout*/) override {
        if (req.method() == bh::verb::put) {
            co_return http::downloaded_response{
              token_status, make_body(token_body)};
        }
        get_had_token_header = req.find("X-aws-ec2-metadata-token")
                               != req.end();
        co_return http::downloaded_response{meta_status, make_body(meta_body)};
    }

    ss::future<> shutdown_and_stop() override { return ss::now(); }
};

struct env_guard {
    explicit env_guard(const char* value) {
        ::setenv(instance_type_env_var, value, /*overwrite=*/1);
    }
    env_guard(const env_guard&) = delete;
    env_guard& operator=(const env_guard&) = delete;
    env_guard(env_guard&&) = delete;
    env_guard& operator=(env_guard&&) = delete;
    ~env_guard() { ::unsetenv(instance_type_env_var); }
};

} // namespace

TEST_CORO(instance_type_detector, ec2_happy_path) {
    fake_imds_client client;
    client.meta_body = "m6id.4xlarge";
    auto type = co_await query_ec2_instance_type(
      client, std::chrono::seconds{1});
    ASSERT_TRUE_CORO(type.has_value());
    EXPECT_EQ(*type, "m6id.4xlarge");
    // The GET must carry the IMDSv2 token obtained from the PUT.
    EXPECT_TRUE(client.get_had_token_header);
}

TEST_CORO(instance_type_detector, ec2_token_request_fails) {
    fake_imds_client client;
    client.token_status = bh::status::forbidden;
    auto type = co_await query_ec2_instance_type(
      client, std::chrono::seconds{1});
    EXPECT_FALSE(type.has_value());
}

TEST_CORO(instance_type_detector, ec2_metadata_request_fails) {
    fake_imds_client client;
    client.meta_status = bh::status::not_found;
    auto type = co_await query_ec2_instance_type(
      client, std::chrono::seconds{1});
    EXPECT_FALSE(type.has_value());
}

TEST_CORO(instance_type_detector, env_override_resolves_provider_and_info) {
    env_guard guard{"m6id.16xlarge"};
    ss::abort_source as;
    auto host = co_await detect_instance(as);
    ASSERT_TRUE_CORO(host.has_value());
    EXPECT_EQ(host->provider, cloud_provider::aws);
    EXPECT_EQ(host->instance_type, "m6id.16xlarge");
    ASSERT_TRUE_CORO(host->capacity.has_value());
    EXPECT_EQ(host->capacity->vcpus, 64);
}

TEST_CORO(instance_type_detector, env_override_unknown_type_is_ignored) {
    env_guard guard{"definitely.not.a.type"};
    ss::abort_source as;
    auto host = co_await detect_instance(as);
    EXPECT_FALSE(host.has_value());
}
