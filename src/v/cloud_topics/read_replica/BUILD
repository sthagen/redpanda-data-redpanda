load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/read_replica:__subpackages__"])

redpanda_cc_library(
    name = "stm",
    srcs = [
        "stm.cc",
    ],
    hdrs = [
        "stm.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/serde",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/cluster:state_machine_registry",
        "//src/v/lsm",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/utils:detailed_error",
        "@seastar",
    ],
)
