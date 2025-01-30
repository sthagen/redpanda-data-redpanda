/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "datalake/coordinator/iceberg_snapshot_remover.h"
#include "datalake/table_definition.h"
#include "datalake/tests/test_utils.h"
#include "iceberg/filesystem_catalog.h"
#include "iceberg/remove_snapshots_action.h"
#include "iceberg/table_identifier.h"
#include "iceberg/transaction.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace datalake::coordinator;
using namespace std::chrono_literals;

namespace {
constexpr std::string_view table{"remover-test-table"};
const model::topic test_topic{table};
const iceberg::table_identifier test_table_id{
  .ns = {"redpanda"}, .table = ss::sstring{table}};
} // anonymous namespace

class SnapshotRemoverTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    static constexpr std::string_view base_location{"test"};
    SnapshotRemoverTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , catalog(remote(), bucket_name, ss::sstring(base_location))
      , manifest_io(remote(), bucket_name)
      , remover(catalog, manifest_io) {
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }
    ss::future<> create_table(const iceberg::table_identifier& table_id) {
        auto res = co_await catalog.load_or_create_table(
          table_id,
          datalake::schemaless_struct_type(),
          datalake::hour_partition_spec());
        ASSERT_FALSE_CORO(res.has_error());
    }

    ss::sstring make_filename(size_t i) { return fmt::format("file-{}", i); }
    ss::sstring make_url(size_t i) { return "/" + make_filename(i); }
    // Uploads the given number of files and adds them to the given table.
    ss::future<>
    add_snapshots(const iceberg::table_identifier& table_id, size_t num_files) {
        for (size_t i = 0; i < num_files; ++i) {
            co_await add_snapshot(table_id);
        }
    }

    ss::future<> add_snapshot(const iceberg::table_identifier& table_id) {
        chunked_vector<iceberg::data_file> files;
        auto filename = make_filename(file_counter_++);
        auto file_uri = manifest_io.to_uri({filename});

        iceberg::partition_key pk;
        pk.val = std::make_unique<iceberg::struct_value>();
        pk.val->fields.emplace_back(iceberg::int_value{0});
        files.emplace_back(iceberg::data_file{
          .file_path = file_uri,
          .partition = std::move(pk),
          .file_size_bytes = 0,
        });

        auto load_res = co_await catalog.load_table(table_id);
        ASSERT_FALSE_CORO(load_res.has_error())
          << "Error loading: " << load_res.error();

        auto& table = load_res.value();
        iceberg::transaction txn(std::move(table));
        auto merge_res = co_await txn.merge_append(
          manifest_io, std::move(files));
        ASSERT_FALSE_CORO(merge_res.has_error())
          << "Error appending: " << merge_res.error();

        auto commit_res = co_await catalog.commit_txn(table_id, std::move(txn));
        ASSERT_FALSE_CORO(commit_res.has_error())
          << "Error committing: " << commit_res.error();
    }

    ss::future<chunked_vector<iceberg::snapshot>>
    get_snapshots(const iceberg::table_identifier& table_id) {
        auto load_res = co_await catalog.load_table(table_id);
        chunked_vector<iceberg::snapshot> empty;
        if (load_res.has_error()) {
            co_return std::move(empty);
        }
        auto& table = load_res.value();
        co_return table.snapshots.has_value() ? std::move(*table.snapshots)
                                              : std::move(empty);
    }

    bool has_object(const iceberg::uri& uri) const {
        return get_object("/" + manifest_io.from_uri(uri).value().native())
          .has_value();
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    iceberg::filesystem_catalog catalog;
    iceberg::manifest_io manifest_io;
    iceberg_snapshot_remover remover;

protected:
    size_t file_counter_{0};
};

TEST_F(SnapshotRemoverTest, TestSimpleRemoval) {
    create_table(test_table_id).get();
    add_snapshots(test_table_id, 10).get();
    auto add_ts = model::timestamp::now();

    auto before_snaps = get_snapshots(test_table_id).get();
    ASSERT_EQ(before_snaps.size(), 10);
    for (const auto& s : before_snaps) {
        ASSERT_TRUE(has_object(s.manifest_list_path));
    }

    // Run removal far enough in the future to expire files.
    model::timestamp cleanup_ts{
      add_ts.value()
      + iceberg::remove_snapshots_action::default_max_snapshot_age_ms};
    auto remove_res
      = remover.remove_expired_snapshots(test_topic, cleanup_ts).get();
    ASSERT_FALSE(remove_res.has_error());

    // The manifest lists should be removed.
    size_t num_snaps = 0;
    for (const auto& s : before_snaps) {
        if (has_object(s.manifest_list_path)) {
            ++num_snaps;
        }
    }
    ASSERT_EQ(
      num_snaps,
      iceberg::remove_snapshots_action::default_min_snapshots_retained);
    ASSERT_EQ(num_snaps, get_snapshots(test_table_id).get().size());
}

TEST_F(SnapshotRemoverTest, TestRemovalInParallel) {
    create_table(test_table_id).get();
    add_snapshots(test_table_id, 10).get();
    auto add_ts = model::timestamp::now();
    auto before_snaps = get_snapshots(test_table_id).get();
    ASSERT_EQ(before_snaps.size(), 10);
    for (const auto& s : before_snaps) {
        ASSERT_TRUE(has_object(s.manifest_list_path));
    }
    model::timestamp cleanup_ts{
      add_ts.value()
      + iceberg::remove_snapshots_action::default_max_snapshot_age_ms};

    auto num_fibers = 10;
    std::vector<ss::future<checked<std::nullopt_t, snapshot_remover::errc>>>
      futs;
    futs.reserve(num_fibers);
    for (int i = 0; i < num_fibers; ++i) {
        futs.emplace_back(
          remover.remove_expired_snapshots(test_topic, cleanup_ts));
    }
    // Some may fail because of the contending removals, but the end result
    // should be the same: that the table has had its old snapshots removed.
    ss::when_all_succeed(std::move(futs)).get();

    size_t num_snaps = 0;
    for (const auto& s : before_snaps) {
        if (has_object(s.manifest_list_path)) {
            ++num_snaps;
        }
    }
    ASSERT_EQ(
      num_snaps,
      iceberg::remove_snapshots_action::default_min_snapshots_retained);
    ASSERT_EQ(num_snaps, get_snapshots(test_table_id).get().size());
}

TEST_F(SnapshotRemoverTest, TestRemoveMissingFiles) {
    create_table(test_table_id).get();
    add_snapshots(test_table_id, 10).get();
    auto add_ts = model::timestamp::now();

    auto before_snaps = get_snapshots(test_table_id).get();
    ASSERT_EQ(before_snaps.size(), 10);

    for (const auto& s : before_snaps) {
        auto filename
          = manifest_io.from_uri(s.manifest_list_path).value().native();
        auto url = "/" + filename;
        ASSERT_TRUE(get_object(url).has_value()) << url;

        // Remove the file from s3.
        remove_expectations({filename});

        // Sanity check that we are now missing the file.
        ASSERT_FALSE(has_object(s.manifest_list_path));
    }

    model::timestamp cleanup_ts{
      add_ts.value()
      + iceberg::remove_snapshots_action::default_max_snapshot_age_ms};

    // Run removal far enough in the future to expire files. Even though our
    // files are missing, this should succeed.
    auto remove_res
      = remover.remove_expired_snapshots(test_topic, cleanup_ts).get();
    ASSERT_FALSE(remove_res.has_error());

    ASSERT_EQ(
      iceberg::remove_snapshots_action::default_min_snapshots_retained,
      get_snapshots(test_table_id).get().size());
}

TEST_F(SnapshotRemoverTest, TestRemoveFromMissingTable) {
    auto remove_res
      = remover.remove_expired_snapshots(test_topic, model::timestamp::now())
          .get();
    ASSERT_TRUE(remove_res.has_error());
    ASSERT_EQ(remove_res.error(), snapshot_remover::errc::failed);
}
