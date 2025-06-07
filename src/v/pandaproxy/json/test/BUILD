load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "iobuf",
    timeout = "short",
    srcs = [
        "iobuf.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/json",
        "//src/v/kafka/protocol",
        "//src/v/pandaproxy:core",
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy:parsing",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "parse_rpbench",
    timeout = "short",
    srcs = [
        "parse.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/json",
        "//src/v/kafka/client",
        "//src/v/pandaproxy:core",
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy:parsing",
        "//src/v/pandaproxy:rest_requests",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
