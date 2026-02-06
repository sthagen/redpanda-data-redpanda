/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "test_utils/test_env.h"

#include "absl/strings/match.h"
#include "random/generators.h"

#include <cstdlib>
#include <filesystem>
#include <string>

namespace test_env {

namespace {
// return the temporary test directory if set, else empty string
std::string test_tmpdir() {
    auto tmpdir = std::getenv("TEST_TMPDIR");
    return tmpdir ? tmpdir : "";
}
} // namespace

// Return a randomly named path under the test temporary directory.
// No directory is created.
std::string random_dir_path(std::string prefix, size_t suffix_len) {
    // we need with_random_seed here because multiple calls within a single
    // test process (in different test cases) should get different paths.
    std::string suffix
      = random_generators::with_random_seed().gen_alphanum_string(suffix_len);

    return std::filesystem::path(test_tmpdir()) / (prefix + suffix);
}

// Return the value of the given environment variable, or the given
// default value if the variable is not set.
std::string getenv(
  std::string_view name, // NOLINT(bugprone-easily-swappable-parameters)
  std::string_view default_value) noexcept {
    const char* v = std::getenv(std::string{name}.c_str());
    return v ? v : std::string{default_value};
}

std::string getenv_default(
  std::string_view name_sv, // NOLINT(bugprone-easily-swappable-parameters)
  std::string_view default_value) noexcept {
    std::string name{name_sv};
    const char* v = std::getenv(name.c_str());
    return v ? v : test_env::getenv(name + "_DEFAULT", default_value);
}

bool is_on_ci() noexcept {
    const char* ci_env = std::getenv("CI");
    return ci_env && absl::EqualsIgnoreCase(ci_env, "true");
}

} // namespace test_env
