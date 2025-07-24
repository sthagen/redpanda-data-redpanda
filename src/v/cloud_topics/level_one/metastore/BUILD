load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "metastore",
    hdrs = [
        "metastore.h",
    ],
    include_prefix = "cloud_topics/level_one/metastore",
    visibility = ["//visibility:public"],
    deps = [
        ":offset_interval_set",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "simple",
    srcs = [
        "simple_metastore.cc",
    ],
    hdrs = [
        "simple_metastore.h",
    ],
    implementation_deps = [
        ":state_update",
        "//src/v/cloud_topics:logger",
    ],
    include_prefix = "cloud_topics/level_one/metastore",
    visibility = ["//visibility:public"],
    deps = [
        ":metastore",
        ":state",
        "//src/v/base",
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
    include_prefix = "cloud_topics/level_one/metastore",
    deps = [
        ":offset_interval_set",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object_id",
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
    include_prefix = "cloud_topics/level_one/metastore",
    deps = [
        ":state",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
    ],
)

redpanda_cc_library(
    name = "offset_interval_set",
    srcs = [
        "offset_interval_set.cc",
    ],
    hdrs = [
        "offset_interval_set.h",
    ],
    include_prefix = "cloud_topics/level_one/metastore",
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/container:interval_set",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:map",
    ],
)
