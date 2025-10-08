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

#include "test_utils/global_test_hooks.h"

#include "random/generators.h"
#include "random/test_seeding.h"
#include "test_utils/test_env.h"

#include <fmt/core.h>

#include <cstdlib>
#include <string>
#include <string_view>

using namespace std::literals;

namespace test_hooks {

namespace {

constexpr std::string_view debug_hooks_str = "REDPANDA_DEBUG_TEST_HOOKS";

const bool debug_hooks = test_env::getenv(std::string{debug_hooks_str}, "0")
                         != "0";

static void maybe_debug_log(std::string_view hook, std::string_view test_name) {
    if (debug_hooks) {
        fmt::print(
          stderr,
          "{} {} {}: {}\n",
          debug_hooks_str,
          hook,
          test_name,
          random_generators::global_state_string());
    }
}

} // namespace

void before_test_case(const std::string& test_name) {
    // reset seeds for each test case to improve reproducibility, see
    // https://redpandadata.atlassian.net/wiki/spaces/CORE/pages/1406271495/Random+values+in+tests
    random_generators::reset_seed_for_tests();
    maybe_debug_log("before_test_case", test_name);
}

void after_test_case(const std::string& test_name) {
    maybe_debug_log("after_test_case", test_name);
}
} // namespace test_hooks
