/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/reconciler/reconciliation_source.h"
#include "cloud_topics/reconciler/tests/test_utils.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "test_utils/metrics.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>

using namespace cloud_topics;
using cloud_topics::reconciler::test::fake_source;
using cloud_topics::reconciler::test::unreliable_io;

namespace {

class ReconcilerMetricsTest : public testing::Test {
public:
    ReconcilerMetricsTest() { _reconciler.setup_metrics_for_tests(); }

    ss::shared_ptr<fake_source> add_source() {
        auto ntp = model::random_ntp();
        auto tid = model::create_topic_id();
        auto tidp = model::topic_id_partition(tid, ntp.tp.partition);
        auto src = ss::make_shared<fake_source>(ntp, tidp);
        _reconciler.attach_source(src);
        return src;
    }

    void reconcile() { _reconciler.reconcile().get(); }

    unreliable_io& io() { return _io; }

private:
    unreliable_io _io;
    l1::simple_metastore _metastore;
    reconciler::reconciler _reconciler{&_io, &_metastore};
};

using ::testing::Gt;
using ::testing::Optional;

std::optional<uint64_t> get_reconciliation_rounds() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_rounds");
}

std::optional<uint64_t> get_objects_uploaded() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_objects_uploaded");
}

std::optional<uint64_t> get_bytes_reconciled() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_bytes_reconciled");
}

std::optional<uint64_t> get_batches_reconciled() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_batches_reconciled");
}

std::optional<uint64_t> get_partitions_reconciled() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_partitions_reconciled");
}

std::optional<uint64_t> get_object_build_failed() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_object_build_failed");
}

std::optional<uint64_t> get_object_upload_failed() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_object_upload_failed");
}

std::optional<uint64_t> get_empty_objects_skipped() {
    return test_utils::find_metric_value<uint64_t>(
      "cloud_topics_reconciler_empty_objects_skipped");
}

} // namespace

TEST_F(ReconcilerMetricsTest, ThroughputCounters) {
    EXPECT_THAT(get_reconciliation_rounds(), Optional(0));
    EXPECT_THAT(get_objects_uploaded(), Optional(0));
    EXPECT_THAT(get_bytes_reconciled(), Optional(0));
    EXPECT_THAT(get_batches_reconciled(), Optional(0));
    EXPECT_THAT(get_partitions_reconciled(), Optional(0));
    EXPECT_THAT(get_object_build_failed(), Optional(0));
    EXPECT_THAT(get_object_upload_failed(), Optional(0));
    EXPECT_THAT(get_empty_objects_skipped(), Optional(0));

    auto src1 = add_source();
    auto src2 = add_source();

    src1->add_batch({.count = 10});
    src1->add_batch({.count = 10});
    src1->add_batch({.count = 10});
    src2->add_batch({.count = 10});
    src2->add_batch({.count = 10});

    reconcile();

    EXPECT_THAT(get_reconciliation_rounds(), Optional(1));
    EXPECT_THAT(get_objects_uploaded(), Optional(1));
    EXPECT_THAT(get_partitions_reconciled(), Optional(2));
    EXPECT_THAT(get_batches_reconciled(), Optional(5));
    EXPECT_THAT(get_bytes_reconciled(), Optional(Gt(0)));
    EXPECT_THAT(get_object_build_failed(), Optional(0));
    EXPECT_THAT(get_object_upload_failed(), Optional(0));
    EXPECT_THAT(get_empty_objects_skipped(), Optional(0));

    auto bytes_after_first = *get_bytes_reconciled();

    reconcile();

    EXPECT_THAT(get_reconciliation_rounds(), Optional(2));
    EXPECT_THAT(get_objects_uploaded(), Optional(1));
    EXPECT_THAT(get_partitions_reconciled(), Optional(2));
    EXPECT_THAT(get_batches_reconciled(), Optional(5));
    EXPECT_THAT(get_bytes_reconciled(), Optional(bytes_after_first));
    EXPECT_THAT(get_object_build_failed(), Optional(0));
    EXPECT_THAT(get_object_upload_failed(), Optional(0));
    EXPECT_THAT(get_empty_objects_skipped(), Optional(1));

    src1->add_batch({.count = 15});

    reconcile();

    EXPECT_THAT(get_reconciliation_rounds(), Optional(3));
    EXPECT_THAT(get_objects_uploaded(), Optional(2));
    EXPECT_THAT(get_partitions_reconciled(), Optional(3));
    EXPECT_THAT(get_batches_reconciled(), Optional(6));
    EXPECT_THAT(get_bytes_reconciled(), Optional(Gt(bytes_after_first)));
    EXPECT_THAT(get_object_build_failed(), Optional(0));
    EXPECT_THAT(get_object_upload_failed(), Optional(0));
    EXPECT_THAT(get_empty_objects_skipped(), Optional(1));
}

TEST_F(ReconcilerMetricsTest, FailedObjectsCounter) {
    EXPECT_THAT(get_object_upload_failed(), Optional(0));

    auto src = add_source();
    src->add_batch({.count = 10});

    io().fail_put_object(true);

    reconcile();

    EXPECT_THAT(get_object_upload_failed(), Optional(1));
    EXPECT_THAT(get_objects_uploaded(), Optional(0));

    io().fail_put_object(false);

    reconcile();

    EXPECT_THAT(get_object_upload_failed(), Optional(1));
    EXPECT_THAT(get_objects_uploaded(), Optional(1));
}
