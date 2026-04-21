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

#include "serde/json/parser.h"
#include "serde/json/tests/test_cases.h"
#include "test_utils/runfiles.h"
#include "test_utils/test.h"
#include "utils/file_io.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

using namespace serde::json;

/// Test suite from https://github.com/nst/JSONTestSuite.
///
/// File-name prefixes encode the expected parser behavior per RFC 8259:
/// - y_ must be accepted
/// - n_ must be rejected
/// - i_ parsers are free to accept or reject
class json_test_suite_test
  : public seastar_test
  , public ::testing::WithParamInterface<std::string> {};

TEST_P_CORO(json_test_suite_test, test_parsing) {
    const auto& test_case_path = GetParam();
    auto filename = std::filesystem::path(test_case_path).filename().string();

    auto contents = co_await read_fully(test_case_path);
    auto parser = serde::json::parser(std::move(contents), parser_config{});

    while (co_await parser.next()) {
    }

    auto final_token = parser.token();
    ASSERT_TRUE_CORO(final_token == token::eof || final_token == token::error)
      << "parser::next() returned false but final token is neither eof nor "
         "error: "
      << final_token;
    bool accepted = final_token == token::eof;

    if (filename.starts_with("y_")) {
        EXPECT_TRUE(accepted) << filename << ": expected accept, got reject";
    } else if (filename.starts_with("n_")) {
        EXPECT_FALSE(accepted) << filename << ": expected reject, got accept";
    } else {
        vassert(
          filename.starts_with("i_"),
          "Unexpected test case name prefix: {}",
          filename);
        // Implementation-defined: either outcome is acceptable. The parser
        // must not crash.
    }
}

INSTANTIATE_TEST_SUITE_P(
  json_test_suite,
  json_test_suite_test,
  ::testing::ValuesIn(
    serde::json::testing::collect_json_test_cases(
      test_utils::get_runfile_path("test_parsing", "nst_json_test_suite"))),
  [](const ::testing::TestParamInfo<std::string>& info) {
      // GTest requires parameter names to match [a-zA-Z0-9_] and to be
      // unique. Use the filename stem and hex-escape any non-alphanumeric
      // chars (using `_XX`) to preserve uniqueness across similar names.
      auto stem = std::filesystem::path(info.param).stem().string();
      std::string out;
      out.reserve(stem.size());
      for (auto c : stem) {
          auto uc = static_cast<unsigned char>(c);
          if (std::isalnum(uc) || uc == '_') {
              out.push_back(c);
          } else {
              fmt::format_to(std::back_inserter(out), "_{:02x}", uc);
          }
      }
      return out;
  });
