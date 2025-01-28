/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/uri.h"

#include <seastar/testing/perf_tests.hh>

PERF_TEST(from_uri, s3_compat) {
    auto converter = iceberg::uri_converter(cloud_io::s3_compat_provider{
      .scheme = "s3",
    });
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    std::filesystem::path path(
      "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z");
    auto uri = ss::sstring(
      "s3://bucket/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z");

    perf_tests::start_measuring_time();
    auto result = converter.from_uri(bucket, iceberg::uri(uri));
    perf_tests::stop_measuring_time();

    vassert(result.has_value(), "Expected a value");
    vassert(result.value() == path, "Unexpected value");
}
