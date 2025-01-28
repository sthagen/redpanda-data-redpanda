/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/protobuf_utils.h"

namespace datalake {

bool is_recursive_type(
  const google::protobuf::Descriptor& msg,
  const proto_descriptors_stack& stack) {
    return std::any_of(
      stack.begin(), stack.end(), [&](const google::protobuf::Descriptor* d) {
          return d->full_name() == msg.full_name();
      });
}

} // namespace datalake
