/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/transform.h"
#include "transform/tests/test_fixture.h"
#include "transform/transform_processor.h"

#include <gtest/gtest.h>

#include <memory>

namespace transform {
namespace {
class ProcessorTestFixture : public ::testing::Test {
public:
    void SetUp() override {
        ss::shared_ptr<wasm::engine> engine
          = ss::make_shared<testing::fake_wasm_engine>();
        auto src = std::make_unique<testing::fake_source>();
        _src = src.get();
        auto sink = std::make_unique<testing::fake_sink>();
        std::vector<std::unique_ptr<transform::sink>> sinks;
        _sinks.push_back(sink.get());
        sinks.push_back(std::move(sink));
        auto offset_tracker = std::make_unique<testing::fake_offset_tracker>();
        _offset_tracker = offset_tracker.get();
        _p = std::make_unique<transform::processor>(
          testing::my_transform_id,
          testing::my_ntp,
          testing::my_metadata,
          std::move(engine),
          [this](
            model::transform_id,
            const model::ntp&,
            const model::transform_metadata&) { ++_error_count; },
          std::move(src),
          std::move(sinks),
          std::move(offset_tracker),
          &_probe);
        _p->start().get();
        // Wait for the initial offset to be committed so we know that the
        // processor is actually ready, otherwise it could be possible that
        // the processor picks up after the initial records are added to the
        // partition.
        _offset_tracker->wait_for_committed_offset({}).get();
    }
    void TearDown() override { _p->stop().get(); }

    model::record_batch make_tiny_batch() {
        return model::test::make_random_batch(model::test::record_batch_spec{
          .offset = kafka::offset_cast(++_offset),
          .allow_compression = false,
          .count = 1});
    }
    void push_batch(model::record_batch batch) {
        _src->push_batch(std::move(batch)).get();
    }
    model::record_batch read_batch() { return _sinks[0]->read().get(); }
    uint64_t error_count() const { return _error_count; }

    void restart() {
        _p->stop().get();
        _p->start().get();
    }

private:
    static constexpr kafka::offset start_offset = kafka::offset(0);

    kafka::offset _offset = start_offset;
    std::unique_ptr<transform::processor> _p;
    testing::fake_source* _src;
    testing::fake_offset_tracker* _offset_tracker;
    std::vector<testing::fake_sink*> _sinks;
    uint64_t _error_count = 0;
    probe _probe;
};
} // namespace

TEST_F(ProcessorTestFixture, ProcessOne) {
    auto batch = make_tiny_batch();
    push_batch(batch.share());
    auto returned = read_batch();
    EXPECT_EQ(batch, returned);
    EXPECT_EQ(error_count(), 0);
}

TEST_F(ProcessorTestFixture, ProcessMany) {
    std::vector<model::record_batch> batches;
    constexpr int num_batches = 32;
    std::generate_n(std::back_inserter(batches), num_batches, [this] {
        return make_tiny_batch();
    });
    for (auto& b : batches) {
        push_batch(b.share());
    }
    for (auto& b : batches) {
        auto returned = read_batch();
        EXPECT_EQ(b, returned);
    }
    EXPECT_EQ(error_count(), 0);
}

TEST_F(ProcessorTestFixture, TracksOffsets) {
    constexpr int num_batches = 32;
    std::vector<model::record_batch> first_batches;
    std::generate_n(std::back_inserter(first_batches), num_batches, [this] {
        return make_tiny_batch();
    });
    std::vector<model::record_batch> second_batches;
    std::generate_n(std::back_inserter(second_batches), num_batches, [this] {
        return make_tiny_batch();
    });
    for (auto& b : first_batches) {
        push_batch(b.share());
    }
    restart();
    for (auto& b : first_batches) {
        auto returned = read_batch();
        EXPECT_EQ(b, returned);
    }
    restart();
    for (auto& b : second_batches) {
        push_batch(b.share());
    }
    for (auto& b : second_batches) {
        auto returned = read_batch();
        EXPECT_EQ(b, returned);
    }
    EXPECT_EQ(error_count(), 0);
}

} // namespace transform