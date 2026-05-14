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

#include "pandaproxy/default_404_handler.h"

#include "json/document.h"

#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <memory>
#include <string>

namespace pp = pandaproxy;

SEASTAR_THREAD_TEST_CASE(default_404_handler_emits_error_code_envelope) {
    pp::default_404_handler handler{
      ss::sstring{"application/vnd.schemaregistry.v1+json"}};

    auto rep = handler
                 .handle(
                   ss::sstring{},
                   std::make_unique<ss::http::request>(),
                   std::make_unique<ss::http::reply>())
                 .get();

    BOOST_REQUIRE(rep != nullptr);
    BOOST_REQUIRE_EQUAL(
      static_cast<int>(rep->_status),
      static_cast<int>(ss::http::reply::status_type::not_found));

    json::Document doc;
    doc.Parse(rep->_content.c_str(), rep->_content.size());
    BOOST_REQUIRE(!doc.HasParseError());
    BOOST_REQUIRE(doc.IsObject());

    BOOST_REQUIRE(doc.HasMember("error_code"));
    BOOST_REQUIRE(doc["error_code"].IsInt());
    BOOST_REQUIRE_EQUAL(doc["error_code"].GetInt(), 404);

    BOOST_REQUIRE(doc.HasMember("message"));
    BOOST_REQUIRE(doc["message"].IsString());
    BOOST_REQUIRE_EQUAL(
      std::string(doc["message"].GetString()),
      std::string("HTTP 404 Not Found"));

    auto content_type_it = rep->_headers.find("Content-Type");
    BOOST_REQUIRE(content_type_it != rep->_headers.end());
    BOOST_REQUIRE_EQUAL(
      content_type_it->second,
      ss::sstring("application/vnd.schemaregistry.v1+json"));
}
