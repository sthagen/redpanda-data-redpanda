load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "object_id",
    hdrs = [
        "object_id.h",
    ],
    include_prefix = "cloud_topics/level_one/common",
    deps = [
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "object_utils",
    srcs = [
        "object_utils.cc",
    ],
    hdrs = [
        "object_utils.h",
    ],
    include_prefix = "cloud_topics/level_one/common",
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
        "//src/v/cloud_topics/level_zero/reconciler:__subpackages__",
    ],
    deps = [
        ":object_id",
        "//src/v/cloud_storage_clients",
        "//src/v/ssx:sformat",
    ],
)

redpanda_cc_library(
    name = "object",
    srcs = [
        "object.cc",
    ],
    hdrs = [
        "object.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/reflection:type_traits",
        "//src/v/serde:vector",
        "//src/v/storage:record_batch_builder",
    ],
    include_prefix = "cloud_topics/level_one/common",
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
        "@seastar",
    ],
)
