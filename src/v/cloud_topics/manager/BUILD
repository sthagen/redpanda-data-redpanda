load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "manager",
    srcs = [
        "manager.cc",
    ],
    hdrs = [
        "manager.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cluster",
        "//src/v/cluster:notification",
        "//src/v/cluster:topic_configuration",
        "//src/v/cluster/utils:partition_change_notifier_impl",
        "//src/v/raft",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:errc",
        "//src/v/cluster:fwd",
        "//src/v/cluster:ntp_callbacks",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:state_machine_registry",
        "//src/v/cluster:topic_table",
        "//src/v/cluster/utils:partition_change_notifier_api",
        "//src/v/model",
        "@seastar",
    ],
)
