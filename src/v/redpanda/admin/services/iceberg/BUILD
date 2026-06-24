load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "iceberg",
    srcs = ["iceberg.cc"],
    hdrs = ["iceberg.h"],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/v2:iceberg_redpanda_proto",
        "//src/v/cluster:fwd",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/coordinator:model",
        "//src/v/datalake/coordinator:state",
        "//src/v/iceberg:catalog_errors",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
