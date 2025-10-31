load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "converter_test",
    timeout = "short",
    srcs = [
        "converter_test.cc",
    ],
    deps = [
        "//src/v/cluster_link/model",
        "//src/v/crypto",
        "//src/v/redpanda/admin/services/shadow_link:converter",
        "//src/v/test_utils:gtest",
        "//src/v/utils:base64",
        "@googletest//:gtest",
    ],
)
