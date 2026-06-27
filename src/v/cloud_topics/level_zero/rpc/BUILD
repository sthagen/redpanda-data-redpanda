load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

package(default_visibility = ["//src/v/cloud_topics:__subpackages__"])

redpanda_cc_rpc_library(
    name = "generated_l0_rpc",
    src = "rpc.json",
)

redpanda_cc_library(
    name = "rpc_types",
    hdrs = [
        "rpc_types.h",
    ],
    deps = [
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "service",
    srcs = [
        "service.cc",
    ],
    hdrs = [
        "service.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics/level_zero/notifier:level_zero_notifier",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":generated_l0_rpc",
        ":rpc_types",
        "//src/v/base",
        "//src/v/rpc",
        "@seastar",
    ],
)
