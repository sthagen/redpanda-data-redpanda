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

#include <seastar/testing/perf_tests.hh>

PERF_PRE_RUN_HOOK([](const sstring& test_group, const sstring& test_case) {
    test_hooks::before_test_case(test_group + "." + test_case);
});
