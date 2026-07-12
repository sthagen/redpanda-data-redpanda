/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/admission_control_types.h"
#include "test_utils/test.h"

#include <string_view>

using namespace cloud_io;

TEST(reservation_target_spec, try_parse) {
    // Each known group name parses to its group_id.
    EXPECT_EQ(
      try_parse_target_spec("producer_upload:2"),
      (group_target{group_id::producer_upload, 2}));
    EXPECT_EQ(
      try_parse_target_spec("consumer_fetch:7"),
      (group_target{group_id::consumer_fetch, 7}));
    EXPECT_EQ(
      try_parse_target_spec("default_group:0"),
      (group_target{group_id::default_group, 0}));

    // uint32_t boundary: max parses, one past it is out of range.
    EXPECT_EQ(
      try_parse_target_spec("default_group:4294967295"),
      (group_target{group_id::default_group, 4294967295}));
    EXPECT_EQ(try_parse_target_spec("default_group:4294967296"), std::nullopt);

    // Malformed specs and unknown names all fail the parse.
    for (const std::string_view bad : {
           "consumer_fech:2",     // typo'd group name
           "producer_upload",     // no colon
           "producer_upload:",    // empty value
           ":2",                  // empty name
           "",                    // empty spec
           "producer_upload:abc", // non-numeric value
           "producer_upload:2x",  // trailing junk
           "producer_upload:-1",  // negative value
           "producer_upload: 2",  // leading space in value
         }) {
        EXPECT_EQ(try_parse_target_spec(bad), std::nullopt)
          << "expected nullopt for spec: '" << bad << "'";
    }
}
