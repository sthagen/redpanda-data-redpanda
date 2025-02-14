load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "self_test_config_test",
    timeout = "short",
    srcs = ["self_test_config_test.cc"],
    deps = [
        "//src/v/cluster",
        "//src/v/json",
        "//src/v/test_utils:seastar_boost",
        "@boost//:math",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "self_test_request_test",
    timeout = "short",
    srcs = ["self_test_request_test.cc"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cluster",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
