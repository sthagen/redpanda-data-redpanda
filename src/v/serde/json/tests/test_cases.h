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

#pragma once

#include "base/vassert.h"

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

namespace serde::json::testing {

/// Returns the sorted list of full paths to `.json` files found directly
/// under `dir`. The directory is required to exist and to contain at
/// least one `.json` file — this helper is intended to back
/// INSTANTIATE_TEST_SUITE_P calls where an empty list would silently
/// produce zero test cases. Full paths (rather than bare filenames) are
/// returned so callers don't have to carry the directory through to the
/// test body.
inline std::vector<std::string>
collect_json_test_cases(const std::filesystem::path& dir) {
    vassert(
      std::filesystem::exists(dir),
      "Directory does not exist: {}",
      dir.string());
    vassert(
      std::filesystem::is_directory(dir),
      "Path is not a directory: {}",
      dir.string());

    std::vector<std::string> test_cases;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        vassert(
          entry.is_regular_file() || entry.is_symlink(),
          "Expected only files under {}, found: {}",
          dir.string(),
          entry.path().string());
        if (entry.path().extension() == ".json") {
            test_cases.push_back(entry.path().string());
        }
    }

    vassert(
      !test_cases.empty(), "No test cases found in directory {}", dir.string());

    std::ranges::sort(test_cases);
    return test_cases;
}

} // namespace serde::json::testing
