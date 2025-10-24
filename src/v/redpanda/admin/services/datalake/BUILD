load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "datalake",
    srcs = ["datalake.cc"],
    hdrs = ["datalake.h"],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/internal/datalake/v1:datalake_redpanda_proto",
        "//src/v/container:chunked_hash_map",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/coordinator:model",
        "//src/v/datalake/coordinator:state",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
