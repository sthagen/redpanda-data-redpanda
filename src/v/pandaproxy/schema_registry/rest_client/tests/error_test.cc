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

#include "http/request_builder.h"
#include "pandaproxy/schema_registry/rest_client/error.h"
#include "pandaproxy/schema_registry/rest_client/parse.h"
#include "pandaproxy/schema_registry/types.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>
#include <vector>

// These tests pin the display/log form of the error types. Nothing else formats
// a domain_error, so without them the error_kind mapping and every variant's
// format_to path go unexercised even though the values are asserted elsewhere.

namespace pandaproxy::schema_registry::rest_client {

using ::testing::HasSubstr;

namespace {
ss::sstring fmt_error(domain_error err) { return fmt::format("{}", err); }
} // namespace

TEST(error_kind_test, to_string_view_covers_every_kind) {
    using enum error_kind;
    EXPECT_EQ(to_string_view(permanent_failure), "permanent_failure");
    EXPECT_EQ(to_string_view(aborted), "aborted");
    EXPECT_EQ(to_string_view(retriable_http_status), "retriable_http_status");
    EXPECT_EQ(to_string_view(network_error), "network_error");
    EXPECT_EQ(to_string_view(timeout), "timeout");
    // The fmt formatter (via format_to) mirrors to_string_view.
    EXPECT_EQ(fmt::format("{}", network_error), "network_error");
}

TEST(error_kind_test, is_retriable_classification) {
    using enum error_kind;
    EXPECT_TRUE(is_retriable(retriable_http_status));
    EXPECT_TRUE(is_retriable(network_error));
    EXPECT_TRUE(is_retriable(timeout));
    EXPECT_FALSE(is_retriable(permanent_failure));
    EXPECT_FALSE(is_retriable(aborted));
}

TEST(http_status_error_test, format_includes_error_code_and_body) {
    // Both optional fields present: exercises the error_code and body arms of
    // http_status_error::format_to.
    http_status_error with_all{
      .status = boost::beast::http::status::not_found,
      .body = "the body",
      .error_code = 40401,
      .message = "not found"};
    auto s = fmt::format("{}", with_all);
    EXPECT_THAT(s, HasSubstr("error_code=40401"));
    EXPECT_THAT(s, HasSubstr("the body"));

    // Neither optional field: no error_code suffix, no body suffix.
    http_status_error bare{
      .status = boost::beast::http::status::service_unavailable, .body = ""};
    auto bare_s = fmt::format("{}", bare);
    EXPECT_THAT(bare_s, ::testing::Not(HasSubstr("error_code=")));
}

TEST(domain_error_test, format_url_build_error) {
    EXPECT_THAT(
      fmt_error(domain_error{http::url_build_error{"bad url"}}),
      HasSubstr("url_build_error: bad url"));
}

TEST(domain_error_test, format_parse_error) {
    EXPECT_THAT(
      fmt_error(domain_error{parse_error{.reason = "expected array"}}),
      HasSubstr("parse_error: expected array"));
}

TEST(domain_error_test, format_http_call_error_status_and_string) {
    // The http_status_error arm of http_call_error.
    EXPECT_THAT(
      fmt_error(
        domain_error{http_call_error{http_status_error{
          .status = boost::beast::http::status::bad_gateway,
          .body = "upstream"}}}),
      HasSubstr("http_call_error"));
    // The bare-string arm (a transport exception rendered to text).
    EXPECT_THAT(
      fmt_error(domain_error{http_call_error{ss::sstring{"connection reset"}}}),
      HasSubstr("connection reset"));
}

TEST(domain_error_test, format_retries_exhausted_lists_every_reason) {
    using enum error_kind;
    // All reason kinds in one list drives the error_kind formatter (via
    // fmt::join) across every enumerator, and last_error drives that arm.
    retries_exhausted ex{
      .reasons
      = {permanent_failure, aborted, retriable_http_status, network_error, timeout},
      .last_error = http_call_error{ss::sstring{"last one"}}};
    auto s = fmt_error(domain_error{ex});
    EXPECT_THAT(s, HasSubstr("retries_exhausted"));
    EXPECT_THAT(s, HasSubstr("network_error"));
    EXPECT_THAT(s, HasSubstr("timeout"));
    EXPECT_THAT(s, HasSubstr("last_error"));

    // The no-last_error arm: reasons still render, no last_error suffix.
    retries_exhausted none{
      .reasons = {network_error}, .last_error = std::nullopt};
    EXPECT_THAT(
      fmt_error(domain_error{none}), ::testing::Not(HasSubstr("last_error")));
}

TEST(domain_error_test, format_typed_not_found_variants) {
    auto subject = context_subject::unqualified("orders");
    EXPECT_THAT(
      fmt_error(domain_error{subject_not_found{subject}}),
      HasSubstr("subject not found"));
    EXPECT_THAT(
      fmt_error(domain_error{version_not_found{subject, schema_version{7}}}),
      HasSubstr("version not found"));
    EXPECT_THAT(
      fmt_error(domain_error{subject_mode_not_found{subject}}),
      HasSubstr("subject mode not configured"));
    EXPECT_THAT(
      fmt_error(domain_error{subject_config_not_found{subject}}),
      HasSubstr("subject config not configured"));
    EXPECT_THAT(
      fmt_error(domain_error{schema_id_not_found{schema_id{42}}}),
      HasSubstr("schema id not found"));
}

TEST(domain_error_test, format_aborted_error) {
    EXPECT_THAT(
      fmt_error(domain_error{aborted_error{"shutting down"}}),
      HasSubstr("aborted_error: shutting down"));
}

} // namespace pandaproxy::schema_registry::rest_client
