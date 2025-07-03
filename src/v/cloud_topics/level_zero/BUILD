load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "ctp_stm_commands",
    hdrs = ["ctp_stm_commands.h"],
    include_prefix = "cloud_topics/level_zero",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/cloud_topics:dl_version",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_offsets",
    hdrs = ["ctp_stm_offsets.h"],
    include_prefix = "cloud_topics/level_zero",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/cloud_topics:dl_version",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "ctp_stm_state",
    srcs = ["ctp_stm_state.cc"],
    hdrs = ["ctp_stm_state.h"],
    include_prefix = "cloud_topics/level_zero",
    visibility = [":__subpackages__"],
    deps = [
        ":ctp_stm_offsets",
        "//src/v/cloud_topics:dl_snapshot",
        "//src/v/cloud_topics:dl_version",
        "//src/v/container:fragmented_vector",
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
    include_prefix = "cloud_topics/level_zero",
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
    include_prefix = "cloud_topics/level_zero",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:dl_snapshot",
        "//src/v/cloud_topics:dl_version",
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
    include_prefix = "cloud_topics/level_zero",
    visibility = ["//visibility:public"],
    deps = ["//src/v/cluster:state_machine_registry"],
)
