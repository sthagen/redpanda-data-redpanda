/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/vassert.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/client_utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"

#include <seastar/testing/perf_tests.hh>

#include <absl/strings/escaping.h>

namespace pps = pandaproxy::schema_registry;

namespace {

ss::sstring make_proto_schema(const pps::subject& sub, int n_fields) {
    ss::sstring body = ss::format(
      "syntax = \"proto3\";\nmessage MyType{} {{\n", sub);
    for (int32_t i = 1; i <= n_fields; ++i) {
        body += ss::format("\tint32 i{} = {};\n", i, i);
    }
    body += "}\n";
    return body;
}

ss::sstring make_payload(
  const ss::sstring& schema,
  pps::schema_type type = pps::schema_type::protobuf) {
    return ss::format(
      R"({{ "schemaType": "{}", "schema": "{}" }})",
      type,
      absl::CEscape(schema));
}

pps::subject make_subject(int i_sub) {
    return pps::subject{ss::format("TestSubject{}", i_sub)};
}

void setup_client(::http::client& client, int n_subjects, int n_versions) {
    for (int i_sub = 0; i_sub < n_subjects; ++i_sub) {
        pps::subject sub = make_subject(i_sub);
        for (int i_ver = 1; i_ver <= n_versions; ++i_ver) {
            auto schema = make_proto_schema(sub, i_ver);
            auto payload = make_payload(schema);
            auto res = post_schema(client, sub, payload);
            vassert(
              res.headers.result() == boost::beast::http::status::ok,
              "Client setup failed");
        }
    }
}

void measure_lookup_schema(
  ::http::client& client,
  const pps::subject& sub,
  const pps::schema_version ver) {
    auto schema = make_proto_schema(sub, ver);
    auto payload = make_payload(schema);
    perf_tests::start_measuring_time();
    auto res = lookup_schema(client, sub, payload);
    perf_tests::stop_measuring_time();
    vassert(
      res.headers.result() == boost::beast::http::status::ok,
      "Schema lookup failed for sub {} version {}",
      sub,
      ver);
}

void measure_post_schema(
  ::http::client& client,
  const pps::subject& sub,
  const pps::schema_version ver) {
    auto schema = make_proto_schema(sub, ver);
    auto payload = make_payload(schema);
    perf_tests::start_measuring_time();
    auto res = post_schema(client, sub, payload);
    perf_tests::stop_measuring_time();
    vassert(
      res.headers.result() == boost::beast::http::status::ok,
      "Schema post failed for sub {} version {}",
      sub,
      ver);
}

} // namespace

class sr_bench_fixture : public pandaproxy_test_fixture {
public:
    sr_bench_fixture()
      : pandaproxy_test_fixture() {}

    sr_bench_fixture(const sr_bench_fixture&) = delete;
    sr_bench_fixture(sr_bench_fixture&&) = delete;
    sr_bench_fixture operator=(const sr_bench_fixture&) = delete;
    sr_bench_fixture operator=(sr_bench_fixture&&) = delete;
    ~sr_bench_fixture() = default;

    template<typename Fn>
    future<void> run_test(int n_subjects, int n_versions, Fn fn) {
        auto client = make_schema_reg_client();
        ss::thread_attributes thread_attr;
        co_await ss::async(thread_attr, [&client, n_subjects, n_versions, fn] {
            setup_client(client, n_subjects, n_versions);
            fn(client);
        });
        co_return;
    }
};

PERF_TEST_C(sr_bench_fixture, lookup_x1_1) {
    co_await run_test(1, 1, [](::http::client& client) {
        measure_lookup_schema(client, make_subject(0), pps::schema_version{1});
    });
}
PERF_TEST_C(sr_bench_fixture, lookup_x1_10) {
    co_await run_test(1, 10, [](::http::client& client) {
        measure_lookup_schema(client, make_subject(0), pps::schema_version{5});
    });
}
PERF_TEST_C(sr_bench_fixture, lookup_x1_100) {
    co_await run_test(1, 100, [](::http::client& client) {
        measure_lookup_schema(client, make_subject(0), pps::schema_version{50});
    });
}

PERF_TEST_C(sr_bench_fixture, lookup_x10_1) {
    co_await run_test(10, 1, [](::http::client& client) {
        measure_lookup_schema(client, make_subject(0), pps::schema_version{1});
    });
}
PERF_TEST_C(sr_bench_fixture, lookup_x10_10) {
    co_await run_test(10, 10, [](::http::client& client) {
        measure_lookup_schema(client, make_subject(0), pps::schema_version{5});
    });
}
PERF_TEST_C(sr_bench_fixture, lookup_x20_20) {
    co_await run_test(20, 20, [](::http::client& client) {
        measure_lookup_schema(client, make_subject(0), pps::schema_version{10});
    });
}

PERF_TEST_C(sr_bench_fixture, post_x1_1) {
    co_await run_test(1, 1, [](::http::client& client) {
        measure_post_schema(client, make_subject(0), pps::schema_version{2});
    });
}
PERF_TEST_C(sr_bench_fixture, post_x1_10) {
    co_await run_test(1, 10, [](::http::client& client) {
        measure_post_schema(client, make_subject(0), pps::schema_version{11});
    });
}
PERF_TEST_C(sr_bench_fixture, post_x1_100) {
    co_await run_test(1, 100, [](::http::client& client) {
        measure_post_schema(client, make_subject(0), pps::schema_version{101});
    });
}

PERF_TEST_C(sr_bench_fixture, post_x10_1) {
    co_await run_test(10, 1, [](::http::client& client) {
        measure_post_schema(client, make_subject(0), pps::schema_version{2});
    });
}
PERF_TEST_C(sr_bench_fixture, post_x10_10) {
    co_await run_test(10, 10, [](::http::client& client) {
        measure_post_schema(client, make_subject(0), pps::schema_version{11});
    });
}
PERF_TEST_C(sr_bench_fixture, post_x20_20) {
    co_await run_test(20, 20, [](::http::client& client) {
        measure_post_schema(client, make_subject(0), pps::schema_version{21});
    });
}
