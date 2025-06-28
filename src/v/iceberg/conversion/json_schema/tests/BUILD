load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "frontend_test",
    timeout = "short",
    srcs = [
        "frontend_test.cc",
    ],
    deps = [
        "//src/v/iceberg/conversion/json_schema:frontend",
        "//src/v/iceberg/conversion/json_schema:ir",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@rapidjson",
        "@seastar",
    ],
)
