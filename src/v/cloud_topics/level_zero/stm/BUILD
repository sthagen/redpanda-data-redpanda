load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    ":__subpackages__",
])

redpanda_cc_library(
    name = "placeholder",
    hdrs = [
        "placeholder.h",
    ],
    include_prefix = "cloud_topics/level_zero/stm",
    visibility = [
        "//src/v/cloud_topics/batch_cache:__pkg__",
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/level_zero/reader:__pkg__",
        "//src/v/cloud_topics/level_zero/reader/tests:__pkg__",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/serde",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "dl_version",
    hdrs = ["dl_version.h"],
    include_prefix = "cloud_topics/level_zero/stm",
    deps = ["//src/v/utils:named_type"],
)

redpanda_cc_library(
    name = "dl_snapshot",
    srcs = [
        "dl_snapshot.cc",
    ],
    hdrs = [
        "dl_snapshot.h",
    ],
    implementation_deps = [
        "@fmt",
    ],
    include_prefix = "cloud_topics/level_zero/stm",
    deps = [
        ":dl_version",
        "//src/v/container:chunked_vector",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_commands",
    hdrs = ["ctp_stm_commands.h"],
    include_prefix = "cloud_topics/level_zero/stm",
    deps = [
        ":dl_version",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_offsets",
    hdrs = ["ctp_stm_offsets.h"],
    include_prefix = "cloud_topics/level_zero/stm",
    deps = [
        ":dl_version",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_state",
    srcs = ["ctp_stm_state.cc"],
    hdrs = ["ctp_stm_state.h"],
    include_prefix = "cloud_topics/level_zero/stm",
    deps = [
        ":ctp_stm_offsets",
        ":dl_snapshot",
        ":dl_version",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ctp_stm",
    srcs = ["ctp_stm.cc"],
    hdrs = ["ctp_stm.h"],
    implementation_deps = [
        ":ctp_stm_commands",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_topics:types",
        "//src/v/serde:map",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
    ],
    include_prefix = "cloud_topics/level_zero/stm",
    visibility = ["//visibility:public"],
    deps = [
        ":ctp_stm_state",
        "//src/v/raft",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_api",
    srcs = ["ctp_stm_api.cc"],
    hdrs = ["ctp_stm_api.h"],
    implementation_deps = [
        ":ctp_stm",
        ":ctp_stm_commands",
        "//src/v/cloud_topics:types",
        "//src/v/raft",
        "//src/v/serde:uuid",
        "//src/v/storage:record_batch_builder",
    ],
    include_prefix = "cloud_topics/level_zero/stm",
    visibility = ["//visibility:public"],
    deps = [
        ":dl_snapshot",
        ":dl_version",
        "//src/v/base",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_factory",
    srcs =
        [
            "ctp_stm_factory.cc",
        ],
    hdrs =
        [
            "ctp_stm_factory.h",
        ],
    implementation_deps = [
        ":ctp_stm",
        "//src/v/cloud_topics:logger",
    ],
    include_prefix = "cloud_topics/level_zero/stm",
    visibility = ["//visibility:public"],
    deps = ["//src/v/cluster:state_machine_registry"],
)
