load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "consumer_group",
    timeout = "short",
    srcs = [
        "consumer_group.cc",
    ],
    deps = [
        "//src/v/http",
        "//src/v/kafka/protocol:join_group",
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy:rest_requests",
        "//src/v/pandaproxy/rest:server",
        "//src/v/pandaproxy/test:fixture",
        "//src/v/pandaproxy/test:utils",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:beast",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "fetch",
    timeout = "short",
    srcs = [
        "fetch.cc",
    ],
    deps = [
        "//src/v/http",
        "//src/v/pandaproxy:json",
        "//src/v/pandaproxy:rest_requests",
        "//src/v/pandaproxy/rest:server",
        "//src/v/pandaproxy/test:fixture",
        "//src/v/pandaproxy/test:utils",
        "//src/v/test_utils:seastar_boost",
        "@boost//:beast",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "list_topics",
    timeout = "short",
    srcs = [
        "list_topics.cc",
    ],
    deps = [
        "//src/v/http",
        "//src/v/pandaproxy/test:fixture",
        "//src/v/pandaproxy/test:utils",
        "//src/v/test_utils:seastar_boost",
        "@boost//:beast",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce",
    timeout = "short",
    srcs = [
        "produce.cc",
    ],
    deps = [
        "//src/v/http",
        "//src/v/pandaproxy/test:fixture",
        "//src/v/pandaproxy/test:utils",
        "//src/v/test_utils:seastar_boost",
        "@boost//:beast",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
