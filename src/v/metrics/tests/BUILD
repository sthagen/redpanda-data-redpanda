load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "host_metrics_watcher_test",
    timeout = "short",
    srcs = [
        "host_metrics_watcher_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/metrics",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "metrics_test",
    timeout = "short",
    srcs = [
        "metrics_test.cc",
    ],
    deps = [
        "//src/v/metrics",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
    ],
)
