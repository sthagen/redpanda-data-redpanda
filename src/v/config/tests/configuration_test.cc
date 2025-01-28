// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/base_property.h"
#include "config/configuration.h"

#include <gtest/gtest.h>

ss::logger lg("config_test"); // NOLINT

namespace config {

// Test that configuration can be round-tripped through YAML. This mirrors the
// config export/import workflow and enables quick iteration on the
// encoding/decoding/etc.
TEST(ConfigurationTest, Roundtrip) {
    auto& cfg = config::shard_local_cfg();
    YAML::Node root_out = to_yaml(cfg, redact_secrets::no);

    lg.debug("Configuration as YAML: {}", root_out);

    try {
        cfg.read_yaml(root_out);
        YAML::Node root_in = to_yaml(cfg, redact_secrets::no);

        // Compare the two YAML strings.
        EXPECT_EQ(fmt::format("{}", root_out), fmt::format("{}", root_in));
    } catch (const std::exception& e) {
        FAIL() << e.what();
    }
}

}; // namespace config
