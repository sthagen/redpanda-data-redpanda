/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "config/node_config.h"
#include "test_utils/tmp_dir.h"

namespace crash_tracker {
class RecorderTestHelper {
public:
    void SetUp() { config::node().data_directory.set_value(_dir.get_path()); }
    void TearDown() {
        _dir.remove().get();
        config::node().data_directory.reset();
    }

private:
    temporary_dir _dir{"recorder_test"};
};
} // namespace crash_tracker
