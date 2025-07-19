load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "domain_supervisor",
    srcs = [
        "domain_supervisor.cc",
    ],
    hdrs = [
        "domain_supervisor.h",
    ],
    include_prefix = "cloud_topics/level_one/domain",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cluster",
        "//src/v/model",
        "@seastar",
    ],
)
