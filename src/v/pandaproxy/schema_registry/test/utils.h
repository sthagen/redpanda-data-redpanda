// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "pandaproxy/schema_registry/seq_writer.h"

class sequence_state_checker_test
  : public pandaproxy::schema_registry::sequence_state_checker {
public:
    explicit sequence_state_checker_test(
      writes_disabled_t wd = writes_disabled_t::no)
      : _wd(wd) {}
    writes_disabled_t writes_disabled() const final { return _wd; }

private:
    writes_disabled_t _wd;
};
