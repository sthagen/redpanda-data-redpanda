load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "syschecks",
    srcs = [
        "hugepages.cc",
        "pidfile.cc",
        "syschecks.cc",
    ],
    hdrs = [
        "hugepages.h",
        "syschecks.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/ssx:sformat",
        "//src/v/version",
        "@seastar",
    ],
)
