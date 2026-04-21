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

#include "serde/json/parser.h"
#include "serde/json/tests/test_cases.h"
#include "test_utils/runfiles.h"
#include "test_utils/test.h"
#include "utils/file_io.h"

#include <filesystem>

using namespace serde::json;

class json_checker_test
  : public seastar_test
  , public ::testing::WithParamInterface<std::string> {};

TEST_P_CORO(json_checker_test, all) {
    const auto& test_case_path = GetParam();
    auto filename = std::filesystem::path(test_case_path).filename().string();

    auto contents = co_await read_fully(test_case_path);
    // Use depth limit of 19 to properly fail fail18.json (20 levels) while
    // allowing pass2.json (19 levels, "Not too deep") to succeed
    auto parser = serde::json::parser(
      std::move(contents), parser_config{.max_depth = 19});

    while (co_await parser.next()) {
        // Do nothing, just drain the parser.
        // We just check if the parser can parse the JSON document
        // successfully or not according to the test case name.
        // The contents are not verified in this test.
    }

    // The file name indicates whether parsing should succeed.
    bool expected_pass = filename.starts_with("pass");
    auto current_token = parser.token();
    if (expected_pass) {
        EXPECT_NE(current_token, token::error)
          << filename << ": expected to pass but failed";
    } else {
        EXPECT_EQ(current_token, token::error)
          << filename << ": expected to fail but passed";
    }
}

INSTANTIATE_TEST_SUITE_P(
  json_checker_tests,
  json_checker_test,
  ::testing::ValuesIn(
    serde::json::testing::collect_json_test_cases(
      test_utils::get_runfile_path(
        "src/v/serde/json/tests/testdata/jsonchecker"))),
  [](const ::testing::TestParamInfo<std::string>& info) {
      return std::filesystem::path(info.param).stem().string();
  });
