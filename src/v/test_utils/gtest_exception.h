/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <gtest/gtest.h>

namespace redpanda::test_utils {

// Custom macro to replace BOOST_REQUIRE_EXCEPTION and
// BOOST_CHECK_EXCEPTION functionality. Verifies that a statement
// throws a specific exception type AND that the exception satisfies a
// predicate.
#define ASSERT_THROWS_WITH_PREDICATE(statement, exception_type, predicate)     \
    try {                                                                      \
        statement;                                                             \
        FAIL() << "Expected " #exception_type " was not thrown";               \
    } catch (const exception_type& e) {                                        \
        if (!(predicate(e))) {                                                 \
            FAIL() << "Exception message did not match predicate: "            \
                   << e.what();                                                \
        }                                                                      \
    } catch (...) {                                                            \
        FAIL() << "Expected " #exception_type " but got different exception";  \
    }

} // namespace redpanda::test_utils
