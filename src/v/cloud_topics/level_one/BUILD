load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "metastore",
    hdrs = [
        "metastore.h",
    ],
    include_prefix = "cloud_topics/level_one",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "simple_metastore",
    srcs = [
        "simple_metastore.cc",
    ],
    hdrs = [
        "simple_metastore.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics/level_one:state_update",
    ],
    include_prefix = "cloud_topics/level_one",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_one:metastore",
        "//src/v/cloud_topics/level_one:state",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state",
    srcs = [
        "state.cc",
    ],
    hdrs = [
        "state.h",
    ],
    include_prefix = "cloud_topics/level_one",
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:set",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state_update",
    srcs = [
        "state_update.cc",
    ],
    hdrs = [
        "state_update.h",
    ],
    include_prefix = "cloud_topics/level_one",
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_one:state",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
    ],
)
