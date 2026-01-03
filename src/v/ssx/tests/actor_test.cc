// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/actor.h"
#include "test_utils/async.h"

#include <seastar/core/future.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

namespace ssx {

using ::testing::ElementsAre;

namespace {

// Test actor that records messages it processes
template<size_t MaxSize, overflow_policy Policy = overflow_policy::block>
class test_actor : public actor<int, MaxSize, Policy> {
public:
    ss::future<> process(int msg) override {
        processed.push_back(msg);
        co_return;
    }

    void on_error(std::exception_ptr) noexcept override {}

    std::vector<int> processed;
};

// Test actor that can throw errors and tracks them
template<size_t MaxSize>
class throwing_actor : public actor<int, MaxSize> {
public:
    ss::future<> process(int msg) override {
        processed.push_back(msg);
        if (msg < 0) {
            throw std::runtime_error("negative value");
        }
        co_return;
    }

    void on_error(std::exception_ptr ex) noexcept override {
        errors.push_back(ex);
    }

    std::vector<int> processed;
    std::vector<std::exception_ptr> errors;
};

} // namespace

TEST(Actor, ProcessesMessagesInOrder) {
    test_actor<10> actor;
    actor.start().get();

    actor.tell(1).get();
    actor.tell(2).get();
    actor.tell(3).get();

    tests::drain_task_queue().get();

    EXPECT_THAT(actor.processed, ElementsAre(1, 2, 3));
    actor.stop().get();
}

TEST(Actor, ErrorsDoNotStopProcessing) {
    throwing_actor<10> actor;
    actor.start().get();

    actor.tell(1).get();
    actor.tell(-1).get(); // This will throw
    actor.tell(2).get();

    tests::drain_task_queue().get();

    EXPECT_THAT(actor.processed, ElementsAre(1, -1, 2));
    EXPECT_EQ(actor.errors.size(), 1);
    actor.stop().get();
}

TEST(Actor, DropOldestPolicy) {
    test_actor<2, overflow_policy::drop_oldest> actor;
    actor.start().get();

    // With drop_oldest, tell() never blocks, so we can fill the queue
    // and subsequent tells will drop the oldest messages
    actor.tell(1).get();
    actor.tell(2).get();
    // Queue is now full, next tell should drop oldest
    actor.tell(3).get(); // Drops 1, adds 3
    actor.tell(4).get(); // Drops 2, adds 4

    tests::drain_task_queue().get();

    // Should have processed 3 and 4 (1 and 2 were dropped)
    EXPECT_THAT(actor.processed, ElementsAre(3, 4));
    actor.stop().get();
}

} // namespace ssx
