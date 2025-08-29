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
        "//src/v/raft",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_zero/gc:level_zero_gc",
        "//src/v/cluster:notification",
        "//src/v/model",
        "@seastar",
    ],
)
