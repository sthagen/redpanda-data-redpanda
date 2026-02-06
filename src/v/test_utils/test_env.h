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
#pragma once

#include <string>

namespace test_env {

// Return a randomly named path under the test temporary directory.
// No directory is created. There is no guarantee the path is unique
// or unused though a sufficiently long random suffix makes that highly
// likely.
//
// Unlike the default seeding policy for random_generators, which uses
// the same seed at the start of each test case, this uses a random seed
// so that multiple calls within a single test process (in different
// test cases) will get different paths.
std::string
random_dir_path(std::string prefix = "test.dir_", size_t suffix_len = 6);

// Return the value of the given environment variable, or the given
// default value if the variable is not set.
std::string
getenv(std::string_view name, std::string_view default_value = "") noexcept;

// Given the name of an environment variable, say X, return the value of the
// environment variable X, if present, otherwise the value of the variable
// X_DEFAULT if present, otherwise default_value.
//
// This falls back to X_DEFAULT to make it easier to set a default value in
// bazel (which sets the _DEFAULT version), and then allow overrides via
// the command line or environment by setting X.
std::string getenv_default(
  std::string_view name, std::string_view default_value = "") noexcept;

// Returns true if running in a CI environment.
// Checks if the CI environment variable is set to "true" (case-insensitive).
bool is_on_ci() noexcept;

} // namespace test_env
