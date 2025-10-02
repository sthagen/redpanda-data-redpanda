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
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <expected>
#include <memory>
#include <utility>

using namespace cloud_topics;

namespace {

class fake_source : public reconciler::source {
public:
    fake_source(model::ntp ntp, model::topic_id_partition tidp)
      : reconciler::source(std::move(ntp), tidp) {}

    void add_batch(
      model::test::record_batch_spec spec,
      std::optional<model::term_id> term = std::nullopt) {
        if (!_source_log.empty()) {
            spec.offset = _source_log.back().last_offset() + model::offset{1};
        }
        auto batch = model::test::make_random_batch(spec);
        if (term.has_value()) {
            batch.set_term(term.value());
        }
        _source_log.push_back(std::move(batch));
    }

    kafka::offset last_reconciled_offset() override { return _lro; }

    ss::future<std::expected<void, errc>>
    set_last_reconciled_offset(kafka::offset o, ss::abort_source&) override {
        if (_fail_set_lro) {
            co_return std::unexpected(errc::failure);
        }
        _lro = o;
        co_return std::expected<void, errc>{};
    }

    ss::future<model::record_batch_reader>
    make_reader(source::reader_config cfg) override {
        if (_fail_make_reader) {
            throw std::runtime_error("Failed to make reader");
        }
        chunked_vector<model::record_batch> log;
        size_t size = 0;
        for (const auto& batch : _source_log) {
            if (
              model::offset_cast(batch.base_offset())
              < last_reconciled_offset()) {
                continue;
            }
            size += batch.size_bytes();
            log.push_back(batch.copy());
            if (size > cfg.max_bytes) {
                break;
            }
        }
        co_return model::make_chunked_memory_record_batch_reader(
          std::move(log));
    }

    void fail_set_lro(bool fail) { _fail_set_lro = fail; }
    void fail_make_reader(bool fail) { _fail_make_reader = fail; }

private:
    kafka::offset _lro;
    chunked_vector<model::record_batch> _source_log;
    bool _fail_set_lro = false;
    bool _fail_make_reader = false;
};

class unreliable_metastore : public l1::simple_metastore {
public:
    ss::future<std::expected<add_response, errc>> add_objects(
      const object_metadata_builder& builder,
      const term_offset_map_t& terms) override {
        if (_fail_add_objects) {
            co_return std::unexpected(errc::invalid_request);
        }
        if (_fail_add_objects_transiently_count > 0) {
            _fail_add_objects_transiently_count--;
            co_return std::unexpected(errc::transport_error);
        }
        co_return co_await l1::simple_metastore::add_objects(builder, terms);
    }

    void fail_add_objects(bool fail) { _fail_add_objects = fail; }
    void fail_add_objects_transiently(int times) {
        _fail_add_objects_transiently_count = times;
    }

private:
    bool _fail_add_objects = false;
    int _fail_add_objects_transiently_count = 0;
};

class unreliable_io : public l1::fake_io {
public:
    ss::future<std::expected<std::unique_ptr<l1::staging_file>, errc>>
    create_tmp_file() override {
        if (_fail_create_tmp_file) {
            co_return std::unexpected(errc::file_io_error);
        }
        co_return co_await l1::fake_io::create_tmp_file();
    }

    ss::future<std::expected<void, errc>> put_object(
      l1::object_id oid,
      l1::staging_file* file,
      ss::abort_source* as) override {
        if (_fail_put_object) {
            co_return std::unexpected(errc::cloud_op_error);
        }
        co_return co_await l1::fake_io::put_object(oid, file, as);
    }

    void fail_create_tmp_file(bool fail) { _fail_create_tmp_file = fail; }
    void fail_put_object(bool fail) { _fail_put_object = fail; }

private:
    bool _fail_create_tmp_file = false;
    bool _fail_put_object = false;
};

class ReconcilerTest : public testing::Test {
public:
    ReconcilerTest()
      : _reconciler(&_io, &_metastore) {}

    ss::shared_ptr<fake_source> add_source() {
        auto ntp = model::random_ntp();
        auto tid = model::create_topic_id();
        auto src = ss::make_shared<fake_source>(
          ntp, model::topic_id_partition{tid, ntp.tp.partition});
        _reconciler.attach_source(src);
        return src;
    }

    void reconcile() { _reconciler.reconcile().get(); }

    std::optional<kafka::offset>
    metastore_next_offset(ss::shared_ptr<fake_source> src) {
        auto offsets = _metastore.get_offsets(src->topic_id_partition()).get();
        if (!offsets.has_value()) {
            return std::nullopt;
        }
        return offsets.value().next_offset;
    }

    unreliable_metastore& metastore() { return _metastore; }
    unreliable_io& io() { return _io; }

protected:
    unreliable_io _io;
    unreliable_metastore _metastore;
    reconciler::reconciler _reconciler;
};

using ::testing::Optional;

} // namespace

TEST_F(ReconcilerTest, EmptySource) {
    auto src = add_source();
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);
}

TEST_F(ReconcilerTest, SingleSource) {
    auto src = add_source();
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{29});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{30}));
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{39});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{40}));
}

TEST_F(ReconcilerTest, MultipleSources) {
    auto src1 = add_source();
    auto src2 = add_source();
    auto src3 = add_source();

    src1->add_batch({.count = 10});
    src1->add_batch({.count = 5});

    src2->add_batch({.count = 20});
    src2->add_batch({.count = 15});
    src2->add_batch({.count = 10});

    src3->add_batch({.count = 8});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{14});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{44});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{7});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{15}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{45}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{8}));

    // Add more data.
    src1->add_batch({.count = 10});

    src2->add_batch({.count = 10});

    src3->add_batch({.count = 10});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{24});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{54});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{17});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{25}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{55}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{18}));

    // Add data to only one of the sources.
    src2->add_batch({.count = 10});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{24});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{64});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{17});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{25}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{65}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{18}));
}

TEST_F(ReconcilerTest, ObjectSizeLimit) {
    auto src = add_source();

    // Total size = 50 * 3 * 512KiB = 75MiB, which is greater than the 64MiB
    // max object size.
    constexpr auto batch_count = 50;
    constexpr auto record_count = 3;
    constexpr auto record_size = 512_KiB;
    for (size_t i = 0; i < batch_count; ++i) {
        src->add_batch(
          {.count = record_count,
           .record_sizes = std::vector<size_t>(record_count, record_size)});
    }

    reconcile();

    // Check that some, but not all, data was reconciled.
    constexpr auto last_offset = batch_count * record_count - 1;
    auto lro = src->last_reconciled_offset();
    EXPECT_GT(lro, kafka::offset{0});
    EXPECT_LT(lro, kafka::offset{last_offset});

    // Reconciling again should process the rest of the data.
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{last_offset});
    EXPECT_THAT(
      metastore_next_offset(src), Optional(kafka::offset{last_offset + 1}));
}

TEST_F(ReconcilerTest, SourceReadFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});
    src->fail_make_reader(true);

    reconcile();

    // The failure of one source should not stop others from being reconciled.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    src->fail_make_reader(false);

    reconcile();

    // Reconciliation should resume on a source after a failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, MetastoreAddObjectsFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});
    metastore().fail_add_objects(true);

    reconcile();

    // Verify LRO doesn't advance when metastore fails, for any source.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    metastore().fail_add_objects(false);

    reconcile();

    // Reconciliation should resume after a metastore failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, MetastoreAddObjectsTransientFailure) {
    auto src1 = add_source();
    auto src2 = add_source();

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 10});

    metastore().fail_add_objects_transiently(3);

    reconcile();

    // A transient failure should be retried until success or timeout, and
    // retries will be too fast to time out in this test.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{10}));
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, IOStagingFileFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});

    io().fail_create_tmp_file(true);

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    io().fail_create_tmp_file(false);

    reconcile();

    // Reconciliation should resume after an IO failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, IOPutObjectFailure) {
    auto src = add_source();
    src->add_batch({.count = 10});

    io().fail_put_object(true);

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);

    io().fail_put_object(false);

    reconcile();

    // Reconciliation should resume after an IO failure.
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{10}));
}

TEST_F(ReconcilerTest, LROUpdateFailure) {
    // Note that this test tests the behavior when the LRO and the metastore's
    // next_offset go out of sync, so it's also a test for recovery in cases
    // like a reconciliation race between an old and new leader.
    auto src1 = add_source();
    auto src2 = add_source();

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 10});

    src1->fail_set_lro(true);

    reconcile();

    // LRO failure doesn't stop metastore update, and it doesn't
    // interfere with reconciliation of other sources.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{});
    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{10}));
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{10}));

    src1->fail_set_lro(false);

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 10});

    reconcile();

    // The reconciler will adjust the LRO based on the metastore's next offset
    // in the next round, but it won't make any more progress.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{9});
    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{10}));
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{19});
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{20}));

    reconcile();

    // In the round after that, reconciliation makes progress.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{19});
    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{20}));
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{19});
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{20}));
}

TEST_F(ReconcilerTest, MultipleSourcesWithFailures) {
    auto src1 = add_source();
    auto src2 = add_source();
    auto src3 = add_source();

    src1->add_batch({.count = 10});
    src2->add_batch({.count = 20});
    src3->add_batch({.count = 30});

    src2->fail_make_reader(true);

    reconcile();

    // When one source in an object fails, then the entire object doesn't fail.
    // NB: This depends on the simple_metastore grouping all sources into the
    //     same object.
    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{9});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{29});

    EXPECT_EQ(metastore_next_offset(src1), kafka::offset{10});
    EXPECT_EQ(metastore_next_offset(src2), std::nullopt);
    EXPECT_EQ(metastore_next_offset(src3), kafka::offset{30});
}

TEST_F(ReconcilerTest, TermTracking) {
    auto src = add_source();

    src->add_batch({.count = 10}, model::term_id{1});
    src->add_batch({.count = 10}, model::term_id{1});
    src->add_batch({.count = 10}, model::term_id{2});
    src->add_batch({.count = 10}, model::term_id{3});

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{39});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{40}));

    auto term_at_5 = metastore()
                       .get_term_for_offset(
                         src->topic_id_partition(), kafka::offset{5})
                       .get();
    auto term_at_25 = metastore()
                        .get_term_for_offset(
                          src->topic_id_partition(), kafka::offset{25})
                        .get();
    auto term_at_35 = metastore()
                        .get_term_for_offset(
                          src->topic_id_partition(), kafka::offset{35})
                        .get();

    EXPECT_TRUE(term_at_5.has_value());
    EXPECT_EQ(term_at_5.value(), model::term_id{1});
    EXPECT_TRUE(term_at_25.has_value());
    EXPECT_EQ(term_at_25.value(), model::term_id{2});
    EXPECT_TRUE(term_at_35.has_value());
    EXPECT_EQ(term_at_35.value(), model::term_id{3});
}

TEST_F(ReconcilerTest, OffsetAndTimestampTracking) {
    auto src = add_source();

    model::timestamp base_ts{1000000};
    src->add_batch({.count = 10, .timestamp = base_ts});
    src->add_batch(
      {.count = 10, .timestamp = model::timestamp{base_ts() + 1000}});

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{19});

    src->add_batch(
      {.count = 10, .timestamp = model::timestamp{base_ts() + 2000}});

    reconcile();

    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{29});

    // Offset queries.
    auto obj_at_offset_0 = metastore()
                             .get_first_ge(
                               src->topic_id_partition(), kafka::offset{0})
                             .get();
    EXPECT_TRUE(obj_at_offset_0.has_value());

    auto obj_at_offset_25 = metastore()
                              .get_first_ge(
                                src->topic_id_partition(), kafka::offset{25})
                              .get();
    EXPECT_TRUE(obj_at_offset_25.has_value());
    EXPECT_NE(obj_at_offset_0.value().oid, obj_at_offset_25.value().oid);

    auto obj_beyond_offset = metastore()
                               .get_first_ge(
                                 src->topic_id_partition(), kafka::offset{1000})
                               .get();
    EXPECT_FALSE(obj_beyond_offset.has_value());
    EXPECT_EQ(obj_beyond_offset.error(), l1::metastore::errc::out_of_range);

    // Timestamp queries.
    auto obj_at_ts
      = metastore().get_first_ge(src->topic_id_partition(), base_ts).get();
    EXPECT_TRUE(obj_at_ts.has_value());

    auto obj_at_later_ts = metastore()
                             .get_first_ge(
                               src->topic_id_partition(),
                               model::timestamp{base_ts() + 1500})
                             .get();
    EXPECT_TRUE(obj_at_later_ts.has_value());
    EXPECT_NE(obj_at_ts.value().oid, obj_at_later_ts.value().oid);

    auto obj_beyond_ts = metastore()
                           .get_first_ge(
                             src->topic_id_partition(),
                             model::timestamp{base_ts() + 10000})
                           .get();
    EXPECT_FALSE(obj_beyond_ts.has_value());
    EXPECT_EQ(obj_beyond_ts.error(), l1::metastore::errc::out_of_range);
}
