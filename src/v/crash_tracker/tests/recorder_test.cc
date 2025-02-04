/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "config/node_config.h"
#include "crash_tracker/recorder.h"
#include "test_utils/tmp_dir.h"

#include <gtest/gtest.h>

#include <exception>
#include <stdexcept>

namespace crash_tracker {

class RecorderTest : public testing::Test {
public:
    void SetUp() override {
        config::node().data_directory.set_value(_dir.get_path());
    }
    void TearDown() override {
        _dir.remove().get();
        config::node().data_directory.reset();
    }

private:
    temporary_dir _dir{"recorder_test"};
};

TEST_F(RecorderTest, TestFileCleanup) {
    const auto test_eptr = std::make_exception_ptr(std::runtime_error{""});

    // Observe no recorded crashes before the first start
    auto crashes = get_test_recorder().get_recorded_crashes().get();
    ASSERT_EQ(crashes.size(), 0);

    // Simulate lots of crashed restarts to generate crash reports on disk
    for (size_t i = 0; i < recorder::crash_files_to_keep + 5; i++) {
        auto rec = get_test_recorder();
        rec.start().get();
        rec.record_crash_exception(test_eptr);
    }

    // Run one more restart and observe that old crash files are cleaned up
    auto rec = get_test_recorder();
    rec.start().get();

    crashes = rec.get_recorded_crashes().get();
    ASSERT_EQ(crashes.size(), recorder::crash_files_to_keep);
}

} // namespace crash_tracker
