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

redpanda_cc_gtest(
    name = "instance_info_test",
    timeout = "short",
    srcs = [
        "instance_info_test.cc",
    ],
    deps = [
        "//src/v/metrics:instance_info",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "instance_metrics_test",
    timeout = "short",
    srcs = [
        "instance_metrics_test.cc",
    ],
    deps = [
        "//src/v/metrics:instance_metrics",
        "//src/v/metrics:instance_type_detector",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:metrics",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "instance_type_detector_test",
    timeout = "short",
    srcs = [
        "instance_type_detector_test.cc",
        "//src/v/metrics:instance_type_detector_impl.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/http",
        "//src/v/metrics:instance_type_detector",
        "//src/v/test_utils:gtest",
        "@boost//:beast",
        "@googletest//:gtest",
        "@seastar",
    ],
)
