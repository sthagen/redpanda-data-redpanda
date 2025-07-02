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
#include "serde/json/tests/data.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace experimental::serde::json;

// Simple test to ensure the parse doesn't fail on valid sample data. The
// contents and correctness is not verified in this test.
TEST_CORO(json_test_suite, parse) {
    auto parser = experimental::serde::json::parser(
      co_await json_test_suite_sample());

    while (co_await parser.next()) {
        // Do nothing, just drain the parser.
        // The contents and correctness is not verified in this test.
    }

    EXPECT_EQ(parser.token(), token::eof) << "Expected to reach EOF but got: "
                                          << std::to_underlying(parser.token());
}

struct token_seq_test_case {
    std::string_view input;
    std::vector<token> expected_tokens;
};

ss::future<> run_test_case(const token_seq_test_case& tc) {
    auto parser = experimental::serde::json::parser(iobuf::from(tc.input));
    for (const auto& expected : tc.expected_tokens) {
        ASSERT_TRUE_CORO(co_await parser.next())
          << "Expected next() to return true for input: " << tc.input;
        ASSERT_EQ_CORO(parser.token(), expected)
          << "Unexpected token for input: " << tc.input;
    }
    ASSERT_FALSE_CORO(co_await parser.next())
      << "Expected next() to return false after all tokens for input: "
      << tc.input;
}

TEST_CORO(json_parser, parse_empty) {
    constexpr auto empty_documents = std::to_array<std::string_view>(
      {"", " ", "\n", "\t", "\r\n"});

    for (const auto& doc : empty_documents) {
        SCOPED_TRACE(fmt::format("Testing empty document: {}", doc));
        ASSERT_NO_FATAL_FAILURE_CORO(co_await run_test_case({
          .input = doc,
          .expected_tokens = {token::error},
        }));
    }
};

TEST_CORO(json_parser, leading_trailing_whitespace) {
    ASSERT_NO_FATAL_FAILURE_CORO(co_await run_test_case({
        .input = "   [   ]   ",
        .expected_tokens = {
            token::start_array,
            token::end_array,
            token::eof,
        },
    }));
}
