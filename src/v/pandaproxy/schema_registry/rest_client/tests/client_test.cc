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
#include "pandaproxy/schema_registry/rest_client/client.h"
#include "pandaproxy/schema_registry/rest_client/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <variant>

namespace rc = pandaproxy::schema_registry::rest_client;
namespace pps = pandaproxy::schema_registry;
namespace bh = boost::beast::http;
using namespace std::chrono_literals;
using namespace testing;

namespace {

constexpr auto endpoint = "http://localhost:8081";

class mock_client : public http::abstract_client {
public:
    MOCK_METHOD(
      ss::future<http::downloaded_response>,
      request_and_collect_response,
      (bh::request_header<>&&,
       std::optional<iobuf>,
       ss::lowres_clock::duration),
      (override));
    MOCK_METHOD(ss::future<>, shutdown_and_stop, (), (override));
};

std::unique_ptr<http::abstract_client>
make_http_client(std::function<void(mock_client&)> set_expectations) {
    auto client = std::make_unique<NiceMock<mock_client>>();
    ON_CALL(*client, shutdown_and_stop()).WillByDefault([] {
        return ss::make_ready_future<>();
    });
    set_expectations(*client);
    return client;
}

// A response action yielding a fixed status and body.
auto respond(bh::status status, std::string_view body) {
    return [status, body = ss::sstring{body}](
             bh::request_header<>&&,
             std::optional<iobuf>,
             ss::lowres_clock::duration) {
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = status, .body = iobuf::from(body)});
    };
}

} // namespace

TEST(rest_client, list_subjects_request_shape_and_success) {
    auto check_and_respond = [](
                               bh::request_header<>&& r,
                               std::optional<iobuf>,
                               ss::lowres_clock::duration) {
        EXPECT_EQ(r.method(), bh::verb::get);
        EXPECT_EQ(r.target(), "/subjects");
        EXPECT_EQ(r.at(bh::field::accept), "application/json");
        // base64("user:pass") == "dXNlcjpwYXNz"
        EXPECT_EQ(r.at(bh::field::authorization), "Basic dXNlcjpwYXNz");
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = bh::status::ok,
            .body = iobuf::from(R"(["s1", ":.ctx:s2"])")});
    };
    rc::client client{
      make_http_client([&](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(check_and_respond);
      }),
      endpoint,
      rc::basic_auth_credentials{.username = "user", .password = "pass"},
      pps::qualified_subjects_enabled::yes};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(
      *res,
      ElementsAre(
        pps::context_subject::unqualified("s1"),
        pps::context_subject(pps::context{".ctx"}, pps::subject{"s2"})));
}

TEST(rest_client, list_subjects_deleted_adds_query_param) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.target(), "/subjects?deleted=true");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok, .body = iobuf::from("[]")});
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc, pps::include_deleted::yes).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
}

TEST(rest_client, list_subjects_context_sends_param_and_filters_client_side) {
    // The client sends ?subjectPrefix=":<ctx>:" AND filters client-side, so a
    // server that ignores the param (like Redpanda, which returns everything
    // here) still yields subjects scoped to exactly the requested context.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                // ':' is percent-encoded; ":.dev:" -> "%3A.dev%3A".
                EXPECT_EQ(r.target(), "/subjects?subjectPrefix=%3A.dev%3A");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(R"(["s1", ":.dev:s2", ":.prod:s3"])")});
            });
      }),
      endpoint,
      std::nullopt,
      pps::qualified_subjects_enabled::yes};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .list_subjects(
                   rtc, pps::include_deleted::no, pps::context{".dev"})
                 .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(
      *res,
      ElementsAre(
        pps::context_subject(pps::context{".dev"}, pps::subject{"s2"})));
}

TEST(rest_client, list_subjects_no_credentials_omits_auth_header) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.count(bh::field::authorization), 0);
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok, .body = iobuf::from("[]")});
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, IsEmpty());
}

TEST(rest_client, list_subjects_retries_then_succeeds) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::service_unavailable, "busy"))
            .WillOnce(respond(bh::status::ok, R"(["s1"])"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 30s, 10ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, SizeIs(1));
}

TEST(rest_client, list_subjects_retries_exhausted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillRepeatedly(respond(bh::status::service_unavailable, "busy"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 10ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::retries_exhausted>(res.error()));
}

TEST(rest_client, list_subjects_transport_exception_is_permanent) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&&,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                return ss::make_exception_future<http::downloaded_response>(
                  std::runtime_error("connection refused"));
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    // A plain runtime_error is classified permanent and surfaces as a string
    // http_call_error.
    ASSERT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
    EXPECT_TRUE(
      std::holds_alternative<ss::sstring>(
        std::get<rc::http_call_error>(res.error())));
}

TEST(rest_client, list_subjects_http_error_attaches_error_code) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40401, "message": "not found"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
    const auto& call = std::get<rc::http_call_error>(res.error());
    ASSERT_TRUE(std::holds_alternative<rc::http_status_error>(call));
    const auto& status = std::get<rc::http_status_error>(call);
    EXPECT_EQ(status.status, bh::status::not_found);
    ASSERT_TRUE(status.error_code.has_value());
    EXPECT_EQ(*status.error_code, 40401);
    ASSERT_TRUE(status.message.has_value());
    EXPECT_EQ(*status.message, "not found");
}

TEST(rest_client, list_subjects_parse_error_surfaced) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"({"not": "an array"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, list_subjects_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc).get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, request_aborted_by_transport_is_aborted) {
    // A transport failure that is an abort/shutdown (here abort_requested) is
    // classified aborted by the retry policy and short-circuits the retry loop
    // as an aborted_error rather than being retried to exhaustion.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&&,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                return ss::make_exception_future<http::downloaded_response>(
                  ss::abort_requested_exception{});
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, request_with_pre_aborted_source_is_aborted) {
    // If the retry chain's abort source is already tripped, the first
    // rtc.retry() raises a shutdown exception before any request is issued; the
    // call resolves as aborted, and the transport is never invoked.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    ss::abort_source as;
    as.request_abort();
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subjects(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, list_contexts_request_shape_and_success) {
    auto check_and_respond = [](
                               bh::request_header<>&& r,
                               std::optional<iobuf>,
                               ss::lowres_clock::duration) {
        EXPECT_EQ(r.method(), bh::verb::get);
        EXPECT_EQ(r.target(), "/contexts");
        EXPECT_EQ(r.at(bh::field::accept), "application/json");
        // base64("user:pass") == "dXNlcjpwYXNz"
        EXPECT_EQ(r.at(bh::field::authorization), "Basic dXNlcjpwYXNz");
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = bh::status::ok, .body = iobuf::from(R"([".", ".dev"])")});
    };
    rc::client client{
      make_http_client([&](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(check_and_respond);
      }),
      endpoint,
      rc::basic_auth_credentials{.username = "user", .password = "pass"}};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_contexts(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, ElementsAre(pps::default_context, pps::context{".dev"}));
}

TEST(rest_client, list_contexts_no_credentials_omits_auth_header) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.count(bh::field::authorization), 0);
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok, .body = iobuf::from(R"(["."])")});
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_contexts(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, ElementsAre(pps::default_context));
}

TEST(rest_client, list_contexts_retries_then_succeeds) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::service_unavailable, "busy"))
            .WillOnce(respond(bh::status::ok, R"(["."])"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 30s, 10ms);
    auto res = client.list_contexts(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, SizeIs(1));
}

TEST(rest_client, list_contexts_parse_error_surfaced) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"({"not": "an array"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_contexts(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, list_contexts_http_error_surfaced) {
    // A permanent HTTP error from perform_request passes through unchanged:
    // contexts has no operation-specific not-found translation.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(
              respond(bh::status::bad_request, R"({"error_code": 40001})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_contexts(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
}

TEST(rest_client, list_contexts_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_contexts(rtc).get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, list_contexts_prefix_sends_param_and_filters_client_side) {
    // The client sends ?contextPrefix= AND filters client-side, so a server
    // that ignores the param (like Redpanda, which returns everything here)
    // still yields correctly-filtered results.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.target(), "/contexts?contextPrefix=.prod");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(
                      R"([".", ".prod", ".prod-eu", ".staging"])")});
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_contexts(rtc, ss::sstring{".prod"}).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(
      *res, ElementsAre(pps::context{".prod"}, pps::context{".prod-eu"}));
}

TEST(rest_client, list_contexts_prefix_matching_nothing_is_empty) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"([".", ".dev"])"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_contexts(rtc, ss::sstring{".zzz"}).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, IsEmpty());
}

TEST(rest_client, list_contexts_prefix_dot_matches_all) {
    // "." is a prefix of every returned context, so nothing is filtered out.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.target(), "/contexts?contextPrefix=.");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(R"([".", ".dev"])")});
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_contexts(rtc, ss::sstring{"."}).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, ElementsAre(pps::default_context, pps::context{".dev"}));
}

TEST(rest_client, get_mode_request_shape_and_success) {
    auto check_and_respond = [](
                               bh::request_header<>&& r,
                               std::optional<iobuf>,
                               ss::lowres_clock::duration) {
        EXPECT_EQ(r.method(), bh::verb::get);
        // No defaultToGlobal query param on the subject-less endpoint.
        EXPECT_EQ(r.target(), "/mode");
        EXPECT_EQ(r.at(bh::field::accept), "application/json");
        // base64("user:pass") == "dXNlcjpwYXNz"
        EXPECT_EQ(r.at(bh::field::authorization), "Basic dXNlcjpwYXNz");
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = bh::status::ok,
            .body = iobuf::from(R"({"mode": "READWRITE"})")});
    };
    rc::client client{
      make_http_client([&](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(check_and_respond);
      }),
      endpoint,
      rc::basic_auth_credentials{.username = "user", .password = "pass"}};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_mode(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->mode, rc::registry_mode::read_write);
    EXPECT_EQ(res->raw, "READWRITE");
}

TEST(rest_client, get_mode_open_enum_tolerates_unknown_values) {
    // A value Redpanda's own enum lacks (FORWARD) and a hypothetical future
    // value both parse: known -> enumerator, unknown -> `unknown` + raw. This
    // is the open-enum contract the client must honor against other registries.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"({"mode": "FORWARD"})"))
            .WillOnce(respond(bh::status::ok, R"({"mode": "GALAXY_BRAIN"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);

    auto fwd = client.get_mode(rtc).get();
    ASSERT_TRUE(fwd.has_value());
    EXPECT_EQ(fwd->mode, rc::registry_mode::forward);
    EXPECT_EQ(fwd->raw, "FORWARD");

    auto unknown = client.get_mode(rtc).get();
    ASSERT_TRUE(unknown.has_value());
    EXPECT_EQ(unknown->mode, rc::registry_mode::unknown);
    EXPECT_EQ(unknown->raw, "GALAXY_BRAIN");

    client.shutdown().get();
}

TEST(rest_client, get_mode_no_credentials_omits_auth_header) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.count(bh::field::authorization), 0);
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(R"({"mode": "READONLY"})")});
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_mode(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->mode, rc::registry_mode::read_only);
}

TEST(rest_client, get_mode_storage_error_is_retried) {
    // The report's one operation-specific failure, 500 / error_code 50001
    // (backend storage), is transient: the client retries it and succeeds once
    // the store recovers.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::internal_server_error,
              R"({"error_code": 50001, "message": "Failed to get mode"})"))
            .WillOnce(respond(bh::status::ok, R"({"mode": "READWRITE"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 30s, 10ms);
    auto res = client.get_mode(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->mode, rc::registry_mode::read_write);
}

TEST(rest_client, get_mode_parse_error_surfaced) {
    // A well-formed but wrong-shaped 200 body (an array, not the mode object)
    // surfaces as a parse_error.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"(["not", "an", "object"])"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_mode(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, get_mode_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_mode(rtc).get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, get_mode_retries_exhausted_surfaces_error) {
    // A persistently failing transient status exhausts the retry budget;
    // get_mode propagates the terminal error from perform_request rather than
    // attempting to parse a body.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillRepeatedly(respond(bh::status::service_unavailable, "busy"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 10ms);
    auto res = client.get_mode(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::retries_exhausted>(res.error()));
}

TEST(rest_client, get_subject_mode_request_shape_and_success) {
    auto check_and_respond = [](
                               bh::request_header<>&& r,
                               std::optional<iobuf>,
                               ss::lowres_clock::duration) {
        EXPECT_EQ(r.method(), bh::verb::get);
        // No defaultToGlobal by default: this asks for the subject's own mode.
        EXPECT_EQ(r.target(), "/mode/orders");
        EXPECT_EQ(r.at(bh::field::accept), "application/json");
        // base64("user:pass") == "dXNlcjpwYXNz"
        EXPECT_EQ(r.at(bh::field::authorization), "Basic dXNlcjpwYXNz");
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = bh::status::ok,
            .body = iobuf::from(R"({"mode": "READONLY"})")});
    };
    rc::client client{
      make_http_client([&](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(check_and_respond);
      }),
      endpoint,
      rc::basic_auth_credentials{.username = "user", .password = "pass"}};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_mode(subject, rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->mode, rc::registry_mode::read_only);
    EXPECT_EQ(res->raw, "READONLY");
}

TEST(rest_client, get_subject_mode_encodes_qualified_subject) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                // ":.ctx:orders" percent-encoded exactly once.
                EXPECT_EQ(r.target(), "/mode/%3A.ctx%3Aorders");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(R"({"mode": "IMPORT"})")});
            });
      }),
      endpoint,
      std::nullopt,
      pps::qualified_subjects_enabled::yes};

    pps::context_subject subject{pps::context{".ctx"}, pps::subject{"orders"}};
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_mode(subject, rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->mode, rc::registry_mode::import);
}

TEST(rest_client, get_subject_mode_default_to_global_adds_query_param) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.target(), "/mode/orders?defaultToGlobal=true");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(R"({"mode": "READWRITE"})")});
            });
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_subject_mode(subject, rtc, pps::default_to_global::yes)
                 .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->mode, rc::registry_mode::read_write);
}

TEST(rest_client, get_subject_mode_no_subject_level_mode) {
    // The operation-specific outcome: 404 / 40409 becomes
    // subject_mode_not_found rather than a generic http error.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40409, "message": "Subject 'orders' does not have subject-level mode configured"})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_mode(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(
      std::holds_alternative<rc::subject_mode_not_found>(res.error()));
    EXPECT_EQ(
      std::get<rc::subject_mode_not_found>(res.error()).subject, subject);
}

TEST(rest_client, get_subject_mode_40401_also_maps_to_not_configured) {
    // Some server versions use 40401 for the missing-subject-mode case; the
    // client treats it the same as 40409 (the report's defensive guidance).
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40401, "message": "Subject 'orders' not found"})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_mode(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(
      std::holds_alternative<rc::subject_mode_not_found>(res.error()));
}

TEST(rest_client, get_subject_mode_other_errors_pass_through_untranslated) {
    // Only 404/40409 (and 40401) map to subject_mode_not_found. A 404 for an
    // unknown path (bare error_code 404) and a non-404 status both stay generic
    // http errors, so a bad URL is not mistaken for a missing subject mode.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 404, "message": "not found"})"))
            .WillOnce(respond(
              bh::status::unprocessable_entity,
              R"({"error_code": 42200, "message": "bad"})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);

    auto bad_path = client.get_subject_mode(subject, rtc).get();
    ASSERT_FALSE(bad_path.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::http_call_error>(bad_path.error()));

    auto unprocessable = client.get_subject_mode(subject, rtc).get();
    ASSERT_FALSE(unprocessable.has_value());
    EXPECT_TRUE(
      std::holds_alternative<rc::http_call_error>(unprocessable.error()));

    client.shutdown().get();
}

TEST(rest_client, get_subject_mode_retries_exhausted_surfaces_error) {
    // A non-http terminal error (retries_exhausted) is not an
    // http_status_error, so translation leaves it untouched and it surfaces
    // as-is.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillRepeatedly(respond(bh::status::service_unavailable, "busy"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 10ms);
    auto res = client.get_subject_mode(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::retries_exhausted>(res.error()));
}

TEST(rest_client, get_subject_mode_transport_exception_passes_through) {
    // A connection-level failure is a string http_call_error (no status), so
    // translation cannot classify it and passes it through unchanged.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&&,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                return ss::make_exception_future<http::downloaded_response>(
                  std::runtime_error("connection refused"));
            });
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_mode(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
    EXPECT_TRUE(
      std::holds_alternative<ss::sstring>(
        std::get<rc::http_call_error>(res.error())));
}

TEST(rest_client, get_subject_mode_parse_error_surfaced) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"(["not", "an", "object"])"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_mode(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, get_subject_mode_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_mode(subject, rtc).get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, get_config_request_shape_and_success) {
    auto check_and_respond = [](
                               bh::request_header<>&& r,
                               std::optional<iobuf>,
                               ss::lowres_clock::duration) {
        EXPECT_EQ(r.method(), bh::verb::get);
        EXPECT_EQ(r.target(), "/config");
        EXPECT_EQ(r.at(bh::field::accept), "application/json");
        // base64("user:pass") == "dXNlcjpwYXNz"
        EXPECT_EQ(r.at(bh::field::authorization), "Basic dXNlcjpwYXNz");
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = bh::status::ok,
            .body = iobuf::from(R"({"compatibilityLevel": "BACKWARD"})")});
    };
    rc::client client{
      make_http_client([&](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(check_and_respond);
      }),
      endpoint,
      rc::basic_auth_credentials{.username = "user", .password = "pass"}};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_config(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->level, rc::registry_compatibility_level::backward);
    EXPECT_EQ(res->raw, "BACKWARD");
    EXPECT_TRUE(res->unknown_fields.empty());
}

TEST(rest_client, get_config_open_enum_tolerates_unknown_value) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(
              respond(bh::status::ok, R"({"compatibilityLevel": "SIDEWAYS"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_config(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->level, rc::registry_compatibility_level::unknown);
    EXPECT_EQ(res->raw, "SIDEWAYS");
}

TEST(rest_client, get_config_records_unmodeled_fields) {
    // A Confluent registry may return a rich object; the client models only
    // compatibilityLevel and names the rest in unknown_fields.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::ok,
              R"({"compatibilityLevel": "FULL", "normalize": true, "validateFields": false})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_config(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->level, rc::registry_compatibility_level::full);
    EXPECT_THAT(
      res->unknown_fields, ElementsAre("normalize", "validateFields"));
}

TEST(rest_client, get_config_no_credentials_omits_auth_header) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.count(bh::field::authorization), 0);
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(R"({"compatibilityLevel": "NONE"})")});
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_config(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->level, rc::registry_compatibility_level::none);
}

TEST(rest_client, get_config_storage_error_is_retried) {
    // The report's one operation-specific failure, 500 / error_code 50001
    // (backend storage), is transient: the client retries and recovers.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::internal_server_error,
              R"({"error_code": 50001, "message": "Failed to get compatibility level"})"))
            .WillOnce(
              respond(bh::status::ok, R"({"compatibilityLevel": "BACKWARD"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 30s, 10ms);
    auto res = client.get_config(rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->level, rc::registry_compatibility_level::backward);
}

TEST(rest_client, get_config_retries_exhausted_surfaces_error) {
    // A persistently failing transient status exhausts the retry budget;
    // get_config propagates the terminal error rather than parsing a body.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillRepeatedly(respond(bh::status::service_unavailable, "busy"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 10ms);
    auto res = client.get_config(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::retries_exhausted>(res.error()));
}

TEST(rest_client, get_config_parse_error_surfaced) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"(["not", "an", "object"])"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_config(rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, get_config_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_config(rtc).get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, get_subject_config_request_shape_and_success) {
    auto check_and_respond = [](
                               bh::request_header<>&& r,
                               std::optional<iobuf>,
                               ss::lowres_clock::duration) {
        EXPECT_EQ(r.method(), bh::verb::get);
        // No defaultToGlobal by default: this asks for the subject's own
        // config.
        EXPECT_EQ(r.target(), "/config/orders");
        EXPECT_EQ(r.at(bh::field::accept), "application/json");
        // base64("user:pass") == "dXNlcjpwYXNz"
        EXPECT_EQ(r.at(bh::field::authorization), "Basic dXNlcjpwYXNz");
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = bh::status::ok,
            .body = iobuf::from(R"({"compatibilityLevel": "NONE"})")});
    };
    rc::client client{
      make_http_client([&](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(check_and_respond);
      }),
      endpoint,
      rc::basic_auth_credentials{.username = "user", .password = "pass"}};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_config(subject, rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->level, rc::registry_compatibility_level::none);
    EXPECT_EQ(res->raw, "NONE");
    EXPECT_TRUE(res->unknown_fields.empty());
}

TEST(rest_client, get_subject_config_encodes_qualified_subject) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                // ":.ctx:orders" percent-encoded exactly once.
                EXPECT_EQ(r.target(), "/config/%3A.ctx%3Aorders");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(R"({"compatibilityLevel": "FULL"})")});
            });
      }),
      endpoint,
      std::nullopt,
      pps::qualified_subjects_enabled::yes};

    pps::context_subject subject{pps::context{".ctx"}, pps::subject{"orders"}};
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_config(subject, rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->level, rc::registry_compatibility_level::full);
}

TEST(rest_client, get_subject_config_default_to_global_adds_query_param) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.target(), "/config/orders?defaultToGlobal=true");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(
                      R"({"compatibilityLevel": "BACKWARD"})")});
            });
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_subject_config(subject, rtc, pps::default_to_global::yes)
                 .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->level, rc::registry_compatibility_level::backward);
}

TEST(rest_client, get_subject_config_no_subject_level_config) {
    // The operation-specific outcome: 404 / 40408 becomes
    // subject_config_not_found rather than a generic http error.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40408, "message": "Subject 'orders' does not have subject-level compatibility configured"})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_config(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(
      std::holds_alternative<rc::subject_config_not_found>(res.error()));
    EXPECT_EQ(
      std::get<rc::subject_config_not_found>(res.error()).subject, subject);
}

TEST(rest_client, get_subject_config_40401_also_maps_to_not_configured) {
    // Some server versions use 40401 for the missing-subject-config case; the
    // client treats it the same as 40408 (the report's defensive guidance).
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40401, "message": "Subject 'orders' not found"})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_config(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(
      std::holds_alternative<rc::subject_config_not_found>(res.error()));
}

TEST(rest_client, get_subject_config_other_errors_pass_through_untranslated) {
    // Only 404/40408 (and 40401) map to subject_config_not_found. A 404 for an
    // unknown path (bare error_code 404) and a non-404 status both stay generic
    // http errors, so a bad URL is not mistaken for a missing subject config.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 404, "message": "not found"})"))
            .WillOnce(respond(
              bh::status::unprocessable_entity,
              R"({"error_code": 42200, "message": "bad"})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);

    auto bad_path = client.get_subject_config(subject, rtc).get();
    ASSERT_FALSE(bad_path.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::http_call_error>(bad_path.error()));

    auto unprocessable = client.get_subject_config(subject, rtc).get();
    ASSERT_FALSE(unprocessable.has_value());
    EXPECT_TRUE(
      std::holds_alternative<rc::http_call_error>(unprocessable.error()));

    client.shutdown().get();
}

TEST(rest_client, get_subject_config_retries_exhausted_surfaces_error) {
    // A non-http terminal error (retries_exhausted) is not an
    // http_status_error, so translation leaves it untouched and it surfaces
    // as-is.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillRepeatedly(respond(bh::status::service_unavailable, "busy"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 10ms);
    auto res = client.get_subject_config(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::retries_exhausted>(res.error()));
}

TEST(rest_client, get_subject_config_transport_exception_passes_through) {
    // A connection-level failure is a string http_call_error (no status), so
    // translation cannot classify it and passes it through unchanged.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&&,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                return ss::make_exception_future<http::downloaded_response>(
                  std::runtime_error("connection refused"));
            });
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_config(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
    EXPECT_TRUE(
      std::holds_alternative<ss::sstring>(
        std::get<rc::http_call_error>(res.error())));
}

TEST(rest_client, get_subject_config_parse_error_surfaced) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"(["not", "an", "object"])"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_config(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, get_subject_config_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.get_subject_config(subject, rtc).get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, list_subject_versions_success_and_encodes_subject) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.method(), bh::verb::get);
                // ":.ctx:orders" must be percent-encoded exactly once.
                EXPECT_EQ(r.target(), "/subjects/%3A.ctx%3Aorders/versions");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from("[1, 2, 3]")});
            });
      }),
      endpoint,
      std::nullopt,
      pps::qualified_subjects_enabled::yes};

    pps::context_subject subject{pps::context{".ctx"}, pps::subject{"orders"}};
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subject_versions(subject, rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(
      *res,
      ElementsAre(
        pps::schema_version{1},
        pps::schema_version{2},
        pps::schema_version{3}));
}

TEST(rest_client, list_subject_versions_deleted_adds_query_param) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.target(), "/subjects/orders/versions?deleted=true");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok, .body = iobuf::from("[1]")});
            });
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .list_subject_versions(subject, rtc, pps::include_deleted::yes)
                 .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
}

TEST(rest_client, list_subject_versions_subject_not_found) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40401, "message": "Subject 'orders' not found."})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subject_versions(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(std::holds_alternative<rc::subject_not_found>(res.error()));
    EXPECT_EQ(std::get<rc::subject_not_found>(res.error()).subject, subject);
}

TEST(rest_client, list_subject_versions_parse_error_surfaced) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"({"not": "an array"})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subject_versions(subject, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, list_subject_versions_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    auto subject = pps::context_subject::unqualified("orders");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client.list_subject_versions(subject, rtc).get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, get_schema_by_version_success) {
    constexpr std::string_view body
      = R"({"subject":"User","version":3,"id":100001,"schemaType":"AVRO",)"
        R"("schema":"{\"type\":\"record\",\"name\":\"User\"}"})";
    rc::client client{
      make_http_client([body](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([body](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.target(), "/subjects/User/versions/3");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok, .body = iobuf::from(body)});
            });
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("User");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_schema_by_version(subject, pps::schema_version{3}, rtc)
                 .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_TRUE(res->unsupported.empty());
    const auto& s = res->schema;
    EXPECT_EQ(s.schema.sub(), subject);
    EXPECT_EQ(s.version, pps::schema_version{3});
    EXPECT_EQ(s.id, pps::schema_id{100001});
}

TEST(rest_client, get_schema_by_version_opts_into_and_ignores_extended_fields) {
    // The client sends Confluent-Accept-Unknown-Properties so the extended
    // fields (guid, ts, deleted) are returned; guid/ts are on the ignorable
    // list and deleted is modeled, so only a real feature (ruleSet) surfaces.
    constexpr std::string_view body
      = R"({"subject":"User","version":1,"id":2,"schema":"x",)"
        R"("guid":"abc","ts":1715000000000,"deleted":false,)"
        R"("ruleSet":{"domainRules":[{"name":"r"}]}})";
    rc::client client{
      make_http_client([body](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([body](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.at("Confluent-Accept-Unknown-Properties"), "true");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok, .body = iobuf::from(body)});
            });
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("User");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_schema_by_version(subject, pps::schema_version{1}, rtc)
                 .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res->unsupported.size(), size_t{1});
    EXPECT_EQ(res->unsupported[0].json_pointer, "/ruleSet");
}

TEST(rest_client, get_schema_by_version_deleted_adds_query_param) {
    constexpr std::string_view body
      = R"({"subject":"User","version":3,"id":100001,"schemaType":"AVRO",)"
        R"("schema":"{\"type\":\"record\",\"name\":\"User\"}"})";
    rc::client client{
      make_http_client([body](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([body](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(r.target(), "/subjects/User/versions/3?deleted=true");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok, .body = iobuf::from(body)});
            });
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("User");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client
          .get_schema_by_version(
            subject, pps::schema_version{3}, rtc, pps::include_deleted::yes)
          .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
}

TEST(rest_client, get_schema_by_version_subject_not_found) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40401, "message": "Subject not found."})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("User");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_schema_by_version(subject, pps::schema_version{5}, rtc)
                 .get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::subject_not_found>(res.error()));
}

TEST(rest_client, get_schema_by_version_version_not_found) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40402, "message": "Version 7 not found."})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("User");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_schema_by_version(subject, pps::schema_version{7}, rtc)
                 .get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(std::holds_alternative<rc::version_not_found>(res.error()));
    const auto& vnf = std::get<rc::version_not_found>(res.error());
    EXPECT_EQ(vnf.subject, subject);
    EXPECT_EQ(vnf.version, pps::schema_version{7});
}

TEST(rest_client, get_schema_by_version_invalid_version_not_translated) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::unprocessable_entity,
              R"({"error_code": 42202, "message": "Invalid version"})"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("User");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_schema_by_version(subject, pps::schema_version{1}, rtc)
                 .get();
    client.shutdown().get();

    // 422 is not a not-found condition: it stays a plain http_status_error.
    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
    const auto& call = std::get<rc::http_call_error>(res.error());
    ASSERT_TRUE(std::holds_alternative<rc::http_status_error>(call));
    EXPECT_EQ(
      std::get<rc::http_status_error>(call).status,
      bh::status::unprocessable_entity);
}

TEST(rest_client, get_schema_by_version_parse_error_surfaced) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"([1, 2, 3])"));
      }),
      endpoint};

    auto subject = pps::context_subject::unqualified("User");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_schema_by_version(subject, pps::schema_version{1}, rtc)
                 .get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, get_schema_by_version_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    auto subject = pps::context_subject::unqualified("User");
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_schema_by_version(subject, pps::schema_version{1}, rtc)
                 .get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}

TEST(rest_client, get_schema_id_subject_versions_request_shape_and_success) {
    auto check_and_respond = [](
                               bh::request_header<>&& r,
                               std::optional<iobuf>,
                               ss::lowres_clock::duration) {
        EXPECT_EQ(r.method(), bh::verb::get);
        // No subject query param -> the default context.
        EXPECT_EQ(r.target(), "/schemas/ids/100001/versions");
        EXPECT_EQ(r.at(bh::field::accept), "application/json");
        // base64("user:pass") == "dXNlcjpwYXNz"
        EXPECT_EQ(r.at(bh::field::authorization), "Basic dXNlcjpwYXNz");
        return ss::make_ready_future<
          http::downloaded_response>(http::downloaded_response{
          .status = bh::status::ok,
          .body = iobuf::from(
            R"([{"subject": "orders", "version": 1}, {"subject": "orders-copy", "version": 3}])")});
    };
    rc::client client{
      make_http_client([&](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(check_and_respond);
      }),
      endpoint,
      rc::basic_auth_credentials{.username = "user", .password = "pass"}};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res = client
                 .get_schema_id_subject_versions(pps::schema_id{100001}, rtc)
                 .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(
      *res,
      ElementsAre(
        pps::subject_version(
          pps::context_subject::unqualified("orders"), pps::schema_version{1}),
        pps::subject_version(
          pps::context_subject::unqualified("orders-copy"),
          pps::schema_version{3})));
}

TEST(
  rest_client, get_schema_id_subject_versions_subject_param_selects_context) {
    // A context_subject is sent verbatim as the `subject` query param,
    // percent-encoded (":" -> "%3A").
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&& r,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                EXPECT_EQ(
                  r.target(),
                  "/schemas/ids/7/versions?subject=%3A.ctx%3Aorders");
                return ss::make_ready_future<http::downloaded_response>(
                  http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(
                      R"([{"subject": ":.ctx:orders", "version": 1}])")});
            });
      }),
      endpoint,
      std::nullopt,
      pps::qualified_subjects_enabled::yes};

    pps::context_subject subject{pps::context{".ctx"}, pps::subject{"orders"}};
    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{7}, rtc, subject)
          .get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(
      *res,
      ElementsAre(
        pps::subject_version(
          pps::context_subject(pps::context{".ctx"}, pps::subject{"orders"}),
          pps::schema_version{1})));
}

TEST(rest_client, get_schema_id_subject_versions_empty_array) {
    // The id exists but no live pair matches: a valid empty result, not a 404.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, "[]"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{1}, rtc).get();
    client.shutdown().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, IsEmpty());
}

TEST(rest_client, get_schema_id_subject_versions_not_found) {
    // 404 / 40403 (schema-not-found) becomes the typed schema_id_not_found.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 40403, "message": "Schema 999 not found"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{999}, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(std::holds_alternative<rc::schema_id_not_found>(res.error()));
    EXPECT_EQ(
      std::get<rc::schema_id_not_found>(res.error()).id, pps::schema_id{999});
}

TEST(rest_client, get_schema_id_subject_versions_bare_404_passes_through) {
    // A bare 404 (e.g. the singular /version path) is not 40403, so it stays a
    // generic http error rather than schema_id_not_found.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::not_found,
              R"({"error_code": 404, "message": "not found"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{1}, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
}

TEST(rest_client, get_schema_id_subject_versions_bad_request_passes_through) {
    // A 400 (e.g. a non-integer id/offset on the server) is not a 404, so it
    // stays a generic http error rather than schema_id_not_found.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(
              bh::status::bad_request,
              R"({"error_code": 400, "message": "bad"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{1}, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
}

TEST(
  rest_client,
  get_schema_id_subject_versions_transport_exception_passes_through) {
    // A connection-level failure is a string http_call_error (no status), so
    // translation cannot classify it and passes it through unchanged.
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce([](
                        bh::request_header<>&&,
                        std::optional<iobuf>,
                        ss::lowres_clock::duration) {
                return ss::make_exception_future<http::downloaded_response>(
                  std::runtime_error("connection refused"));
            });
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{1}, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(std::holds_alternative<rc::http_call_error>(res.error()));
    EXPECT_TRUE(
      std::holds_alternative<ss::sstring>(
        std::get<rc::http_call_error>(res.error())));
}

TEST(rest_client, get_schema_id_subject_versions_retries_exhausted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillRepeatedly(respond(bh::status::service_unavailable, "busy"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 100ms, 10ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{1}, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::retries_exhausted>(res.error()));
}

TEST(rest_client, get_schema_id_subject_versions_parse_error_surfaced) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _))
            .WillOnce(respond(bh::status::ok, R"({"not": "an array"})"));
      }),
      endpoint};

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{1}, rtc).get();
    client.shutdown().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::parse_error>(res.error()));
}

TEST(rest_client, get_schema_id_subject_versions_after_shutdown_is_aborted) {
    rc::client client{
      make_http_client([](mock_client& m) {
          EXPECT_CALL(m, request_and_collect_response(_, _, _)).Times(0);
      }),
      endpoint};

    client.shutdown().get();

    ss::abort_source as;
    retry_chain_node rtc(as, 5s, 100ms);
    auto res
      = client.get_schema_id_subject_versions(pps::schema_id{1}, rtc).get();

    ASSERT_FALSE(res.has_value());
    EXPECT_TRUE(std::holds_alternative<rc::aborted_error>(res.error()));
}
