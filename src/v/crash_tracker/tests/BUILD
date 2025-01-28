load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "limiter_test",
    timeout = "short",
    srcs = [
        "limiter_test.cc",
    ],
    deps = [
        "//src/v/crash_tracker",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
