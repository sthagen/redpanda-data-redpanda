load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "request_shaping_test",
    timeout = "short",
    srcs = [
        "request_shaping_test.cc",
    ],
    deps = [
        "//src/v/cloud_instance_metadata",
        "//src/v/test_utils:gtest",
        "@boost//:beast",
        "@googletest//:gtest",
    ],
)
