load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics:__subpackages__"])

redpanda_cc_library(
    name = "level_zero_notifier",
    srcs = ["level_zero_notifier.cc"],
    hdrs = ["level_zero_notifier.h"],
    deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cluster",
        "//src/v/cluster:fwd",
        "//src/v/cluster:shard_table",
        "//src/v/cluster:state_machine_registry",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)
