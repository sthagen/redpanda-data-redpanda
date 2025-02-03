load("//bazel:build.bzl", "redpanda_cc_binary")
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

redpanda_cc_binary(
    name = "crash_report_generator",
    srcs = ["report_generator.cc"],
    deps = [
        "//src/v/base",
        "//src/v/crash_tracker",
        "//src/v/model",
        "//src/v/utils:file_io",
        "@boost//:program_options",
        "@seastar",
    ],
)
