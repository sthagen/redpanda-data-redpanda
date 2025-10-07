/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed by the
 * Apache License, Version 2.0
 */

#pragma once

#include "absl/time/clock.h"
#include "src/v/redpanda/admin/tests/aip_test_message.proto.h"

inline aip_test::test_message create_test_message(
  int64_t int_val = 42,
  std::string str_val = "test",
  bool bool_val = true,
  std::string nested_name = "nested",
  int32_t nested_val = 100) {
    aip_test::test_message msg;
    msg.set_int_field(int_val);
    msg.set_string_field(ss::sstring(str_val));
    msg.set_bool_field(bool_val);
    msg.set_uint_field(1000);
    msg.set_double_field(3.14);

    msg.get_nested().set_name(ss::sstring(nested_name));
    msg.get_nested().set_value(nested_val);

    msg.set_status(aip_test::test_message_status::status_active);
    msg.set_timestamp_field(absl::Now() - absl::Minutes(5));
    msg.set_duration_field(absl::Seconds(30));

    msg.set_int32_field(123);
    msg.set_float_field(2.5f);

    return msg;
}
