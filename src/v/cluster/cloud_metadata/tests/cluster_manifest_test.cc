/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cluster/cloud_metadata/cluster_manifest.h"

#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

using namespace cluster::cloud_metadata;

namespace {
ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

// Does a roundtrip of JSON serializing and returns the result.
cluster_metadata_manifest roundtrip_json(const cluster_metadata_manifest& m) {
    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    cluster_metadata_manifest restored;
    restored.update(std::move(rstr)).get();
    return restored;
}
} // anonymous namespace

static constexpr std::string_view simple_manifest_json = R"json({
    "version": 0,
    "compat_version": 0,
    "upload_time_since_epoch": 100,
    "cluster_uuid": "01234567-89ab-cdef-0123-456789abcdef",
    "metadata_id": 7,
    "controller_snapshot_offset": 42,
    "controller_snapshot_path": "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/controller.snapshot",
    "offsets_snapshots_by_partition": [["path0"], ["path1"], [], ["path2", "path3"]]
})json";

TEST(cluster_manifest_test, test_basic_serialization) {
    cluster_metadata_manifest manifest;
    manifest.update(make_manifest_stream(simple_manifest_json)).get();
    EXPECT_EQ(100, manifest.upload_time_since_epoch.count());
    auto uuid_str = "01234567-89ab-cdef-0123-456789abcdef";
    EXPECT_EQ(fmt::to_string(manifest.cluster_uuid), uuid_str);
    EXPECT_EQ(7, manifest.metadata_id());
    EXPECT_EQ(42, manifest.controller_snapshot_offset());
    EXPECT_EQ(
      "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/"
      "controller.snapshot",
      manifest.controller_snapshot_path);
    std::vector<std::vector<ss::sstring>> expected_offset_paths{
      {"path0"},
      {"path1"},
      {},
      {"path2", "path3"},
    };
    EXPECT_EQ(expected_offset_paths, manifest.offsets_snapshots_by_partition);

    auto [is, size] = manifest.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    cluster_metadata_manifest restored;
    restored.update(std::move(rstr)).get();
    ASSERT_EQ(manifest, restored);

    cloud_storage::remote_manifest_path path{
      "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/manifests/7/"
      "cluster_manifest.json"};
    ASSERT_EQ(path, manifest.get_manifest_path());
}

static constexpr std::string_view simple_manifest_json_with_extra = R"json({
    "version": 0,
    "compat_version": 0,
    "upload_time_since_epoch": 100,
    "cluster_uuid": "01234567-89ab-cdef-0123-456789abcdef",
    "metadata_id": 7,
    "controller_snapshot_offset": 42,
    "controller_snapshot_path": "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/controller.snapshot",
    "offsets_snapshots_by_partition": [["path0"], ["path1"], [], ["path2", "path3"]],
    "foo": "bar"
})json";

TEST(cluster_manifest_test, test_extra_field) {
    cluster_metadata_manifest manifest;
    manifest.update(make_manifest_stream(simple_manifest_json)).get();

    cluster_metadata_manifest manifest_with_extra;
    manifest_with_extra
      .update(make_manifest_stream(simple_manifest_json_with_extra))
      .get();

    EXPECT_EQ(manifest, manifest_with_extra);
}

static constexpr std::string_view bad_compat_version_json = R"json({
    "version": 0,
    "compat_version": 999,
    "upload_time_since_epoch": 100,
    "cluster_uuid": "01234567-89ab-cdef-0123-456789abcdef",
    "metadata_id": 7,
    "controller_snapshot_offset": 42,
    "controller_snapshot_path": "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/controller.snapshot"
})json";

TEST(cluster_manifest_test, test_bad_compat_serialization) {
    cluster_metadata_manifest manifest;
    EXPECT_THROW(
      manifest.update(make_manifest_stream(bad_compat_version_json)).get(),
      std::runtime_error);
}

static constexpr std::string_view bad_uuid_manifest_json = R"json({
    "version": 0,
    "compat_version": 0,
    "upload_time_since_epoch": 100,
    "cluster_uuid": "1-877-kars4kids",
    "metadata_id": 7,
    "controller_snapshot_offset": 42,
    "controller_snapshot_path": "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/controller.snapshot"
})json";

TEST(cluster_manifest_test, test_bad_uuid) {
    cluster_metadata_manifest manifest;
    EXPECT_THROW(
      manifest.update(make_manifest_stream(bad_uuid_manifest_json)).get(),
      std::runtime_error);
}

static constexpr std::string_view empty_fields_manifest_json = R"json({
    "version": 0,
    "compat_version": 0
})json";

TEST(cluster_manifest_test, test_missing_fields_serialization) {
    cluster_metadata_manifest manifest;
    manifest.update(make_manifest_stream(empty_fields_manifest_json)).get();
    EXPECT_EQ(0, manifest.upload_time_since_epoch.count());
    EXPECT_EQ(model::cluster_uuid{}, manifest.cluster_uuid);
    EXPECT_EQ(cluster_metadata_id{}, manifest.metadata_id);
    EXPECT_EQ(model::offset{}, manifest.controller_snapshot_offset);
    EXPECT_EQ("", manifest.controller_snapshot_path);
}

TEST(cluster_manifest_test, test_max_vals) {
    cluster_metadata_manifest max_manifest;
    max_manifest.cluster_uuid = model::cluster_uuid(uuid_t::create());
    max_manifest.upload_time_since_epoch
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::duration::max());
    max_manifest.metadata_id = cluster_metadata_id::max();
    max_manifest.controller_snapshot_offset = model::offset::max();
    auto max_restored = roundtrip_json(max_manifest);
    ASSERT_EQ(max_manifest, max_restored);
    // Sanity check that serializing the time is sane with respect to now.
    ASSERT_LT(
      ss::lowres_system_clock::now().time_since_epoch().count(),
      max_restored.upload_time_since_epoch.count());
}

TEST(cluster_manifest_test, test_min_vals) {
    cluster_metadata_manifest min_manifest;
    min_manifest.cluster_uuid = model::cluster_uuid(uuid_t::create());
    min_manifest.upload_time_since_epoch
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::duration::min());
    min_manifest.metadata_id = cluster_metadata_id::min();
    min_manifest.controller_snapshot_offset = model::offset::min();
    auto min_restored = roundtrip_json(min_manifest);
    ASSERT_EQ(min_manifest, min_restored);
    // Sanity check that serializing the time is sane with respect to now.
    ASSERT_GT(
      ss::lowres_system_clock::now().time_since_epoch().count(),
      min_restored.upload_time_since_epoch.count());
}
