load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "test_deps",
    srcs = [
        "deps.cc",
    ],
    hdrs = [
        "deps.h",
    ],
    include_prefix = "cluster_link/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/cluster_link/tests:test_lib",
        "//src/v/cluster_link:impl",
        "//src/v/kafka/data/rpc",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "link_test",
    timeout = "short",
    srcs = [
        "link_test.cc",
    ],
    deps = [
        ":test_deps",
        "//src/v/cluster",
        "//src/v/cluster/cluster_link/tests:test_lib",
        "//src/v/cluster_link:impl",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "errc_test",
    timeout = "short",
    srcs = [
        "errc_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:errc",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "base_task_test",
    timeout = "short",
    srcs = [
        "base_task_test.cc",
    ],
    deps = [
        "//src/v/cluster_link:impl",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "task_manager_integration_test",
    timeout = "short",
    srcs = [
        "task_manager_integration_test.cc",
    ],
    flaky = True,
    deps = [
        ":test_deps",
        "//src/v/cluster_link:impl",
        "//src/v/cluster_link/model",
        "//src/v/kafka/data/rpc",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
