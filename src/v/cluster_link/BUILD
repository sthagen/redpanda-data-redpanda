load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "cluster_link",
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "errc",
    srcs = [
        "errc.cc",
    ],
    hdrs = [
        "errc.h",
    ],
    include_prefix = "cluster_link",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "impl",
    srcs = [
        "link.cc",
        "manager.cc",
        "task.cc",
    ],
    hdrs = [
        "link.h",
        "manager.h",
        "task.h",
        "types.h",
    ],
    implementation_deps = [
        ":logger",
    ],
    include_prefix = "cluster_link",
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        ":errc",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster_link/model",
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/data/rpc",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:work_queue",
        "//src/v/utils:mutex",
        "//src/v/utils:notification_list",
        "//src/v/utils:prefix_logger",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    include_prefix = "cluster_link",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "cluster_link",
    srcs = [
        "service.cc",
    ],
    hdrs = [
        "service.h",
    ],
    implementation_deps = [
        ":impl",
        ":logger",
        "//src/v/cluster_link/model",
    ],
    include_prefix = "cluster_link",
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "@seastar",
    ],
)
