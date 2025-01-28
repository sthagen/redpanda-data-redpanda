/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "google/protobuf/descriptor.h"

#include <deque>

namespace datalake {

using proto_descriptors_stack = std::deque<const google::protobuf::Descriptor*>;
inline constexpr int max_recursion_depth = 100;

bool is_recursive_type(
  const google::protobuf::Descriptor& msg,
  const proto_descriptors_stack& stack);

} // namespace datalake
