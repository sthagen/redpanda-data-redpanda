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
    EXPECT_TRUE(res->unknown_fields.empty());
    const auto& s = res->schema;
    EXPECT_EQ(s.schema.sub(), subject);
    EXPECT_EQ(s.version, pps::schema_version{3});
    EXPECT_EQ(s.id, pps::schema_id{100001});
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
