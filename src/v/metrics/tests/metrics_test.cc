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

#include "metrics/metrics.h"
#include "test_utils/scoped_config.h"

#include <gtest/gtest.h>

namespace {

// any_enabled() is the logical OR of "internal endpoint enabled" and "public
// endpoint enabled", so it is only false when both endpoints are disabled.
void set_disabled(scoped_config& sc, bool internal, bool pub) {
    sc.get("disable_metrics").set_value(internal);
    sc.get("disable_public_metrics").set_value(pub);
}

} // namespace

TEST(metrics_enabled, both_enabled) {
    scoped_config sc{};
    set_disabled(sc, /*internal=*/false, /*pub=*/false);
    EXPECT_TRUE(metrics::any_enabled());
}

TEST(metrics_enabled, only_internal_enabled) {
    scoped_config sc{};
    set_disabled(sc, /*internal=*/false, /*pub=*/true);
    EXPECT_TRUE(metrics::any_enabled());
}

TEST(metrics_enabled, only_public_enabled) {
    scoped_config sc{};
    set_disabled(sc, /*internal=*/true, /*pub=*/false);
    EXPECT_TRUE(metrics::any_enabled());
}

TEST(metrics_enabled, both_disabled) {
    scoped_config sc{};
    set_disabled(sc, /*internal=*/true, /*pub=*/true);
    EXPECT_FALSE(metrics::any_enabled());
}
