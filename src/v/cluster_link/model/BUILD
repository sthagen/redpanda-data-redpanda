load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "model",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    include_prefix = "cluster_link/model",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde",
        "//src/v/serde:variant",
        "//src/v/utils:named_type",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)
