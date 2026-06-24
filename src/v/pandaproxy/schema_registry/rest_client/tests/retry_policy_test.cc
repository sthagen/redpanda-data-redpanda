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

#include "pandaproxy/schema_registry/rest_client/retry_policy.h"

#include <gtest/gtest.h>

namespace r = pandaproxy::schema_registry::rest_client;
using enum boost::beast::http::status;

namespace {
template<typename Ex>
r::request_error throw_and_catch(Ex ex) {
    try {
        throw ex;
    } catch (...) {
        return r::default_retry_policy{}.should_retry(std::current_exception());
    }
}
} // namespace

TEST(default_retry_policy, status_ok) {
    r::default_retry_policy p;
    auto result = p.should_retry(
      http::downloaded_response{.status = ok, .body = iobuf::from("success")});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().body, iobuf::from("success"));
}

TEST(default_retry_policy, status_retriable) {
    r::default_retry_policy p;
    for (const auto status : std::to_array(
           {internal_server_error,
            bad_gateway,
            service_unavailable,
            gateway_timeout,
            too_many_requests,
            request_timeout})) {
        auto result = p.should_retry(
          http::downloaded_response{
            .status = status, .body = iobuf::from("retry")});
        ASSERT_FALSE(result.has_value());
        ASSERT_EQ(result.error().kind, r::error_kind::retriable_http_status);
    }
}

TEST(default_retry_policy, status_not_retriable) {
    r::default_retry_policy p;
    for (const auto status : std::to_array(
           {bad_request, unauthorized, method_not_allowed, not_acceptable})) {
        auto result = p.should_retry(
          http::downloaded_response{
            .status = status, .body = iobuf::from("retry")});
        ASSERT_FALSE(result.has_value());
        ASSERT_EQ(result.error().kind, r::error_kind::permanent_failure);
    }
}

TEST(default_retry_policy, boost_system_errors) {
    auto retriable = throw_and_catch(
      boost::system::system_error{boost::beast::http::error::end_of_stream});
    ASSERT_EQ(retriable.kind, r::error_kind::network_error);
    auto permanent_failure = throw_and_catch(
      boost::system::system_error{boost::beast::http::error::bad_alloc});
    ASSERT_EQ(permanent_failure.kind, r::error_kind::permanent_failure);
}

TEST(default_retry_policy, system_errors) {
    auto retriable = throw_and_catch(
      std::system_error{ETIMEDOUT, std::generic_category()});
    ASSERT_EQ(retriable.kind, r::error_kind::network_error);
    auto permanent_failure = throw_and_catch(
      std::system_error{ETIMEDOUT, ss::tls::error_category()});
    ASSERT_EQ(permanent_failure.kind, r::error_kind::permanent_failure);
}

TEST(default_retry_policy, abort_exception) {
    auto gate_failure = throw_and_catch(ss::gate_closed_exception{});
    ASSERT_EQ(gate_failure.kind, r::error_kind::aborted);
    auto abort_failure = throw_and_catch(ss::abort_requested_exception{});
    ASSERT_EQ(abort_failure.kind, r::error_kind::aborted);
}

TEST(default_retry_policy, nested_exception) {
    auto gate_failure = throw_and_catch(
      ss::nested_exception{
        std::make_exception_ptr(ss::gate_closed_exception{}),
        std::make_exception_ptr(std::runtime_error{"out"})});
    ASSERT_EQ(gate_failure.kind, r::error_kind::aborted);
    auto abort_failure = throw_and_catch(
      ss::nested_exception{
        std::make_exception_ptr(std::invalid_argument{""}),
        std::make_exception_ptr(ss::abort_requested_exception{})});
    ASSERT_EQ(abort_failure.kind, r::error_kind::aborted);

    auto result = throw_and_catch(
      ss::nested_exception{
        std::make_exception_ptr(std::invalid_argument{"i"}),
        std::make_exception_ptr(std::invalid_argument{"o"})});
    ASSERT_EQ(result.kind, r::error_kind::permanent_failure);
    ASSERT_TRUE(std::holds_alternative<ss::sstring>(result.err));
    ASSERT_EQ(
      "seastar::nested_exception [outer: std::invalid_argument (o), "
      "inner: std::invalid_argument (i)]",
      std::get<ss::sstring>(result.err));
}
