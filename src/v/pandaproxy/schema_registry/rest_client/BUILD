load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/pandaproxy:__subpackages__"])

redpanda_cc_library(
    name = "parse",
    srcs = [
        "parse.cc",
    ],
    hdrs = [
        "config.h",
        "mode.h",
        "parse.h",
    ],
    implementation_deps = [
        "//src/v/serde/json:parser",
        "//src/v/ssx:sformat",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/strings:string_switch",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "error",
    hdrs = [
        "error.h",
    ],
    visibility = [
        "//src/v/cluster_link:__subpackages__",
        "//src/v/pandaproxy:__subpackages__",
    ],
    deps = [
        ":parse",
        "//src/v/base",
        "//src/v/http:request_builder",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/utils:named_type",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "retry_policy",
    srcs = [
        "retry_policy.cc",
    ],
    hdrs = [
        "retry_policy.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/bytes:iobuf",
    ],
    deps = [
        ":error",
        "//src/v/http",
        "//src/v/net",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "credentials",
    hdrs = [
        "credentials.h",
    ],
    visibility = [
        "//src/v/cluster_link:__subpackages__",
        "//src/v/pandaproxy:__subpackages__",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rate_limited_client",
    srcs = [
        "rate_limited_client.cc",
    ],
    hdrs = [
        "rate_limited_client.h",
    ],
    implementation_deps = [
        ":logger",
    ],
    visibility = [
        "//src/v/cluster_link:__subpackages__",
        "//src/v/pandaproxy:__subpackages__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/http",
        "//src/v/utils:token_bucket",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "pooled_client",
    srcs = [
        "pooled_client.cc",
    ],
    hdrs = [
        "pooled_client.h",
    ],
    visibility = [
        "//src/v/cluster_link:__subpackages__",
        "//src/v/pandaproxy:__subpackages__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/http",
        "//src/v/ssx:semaphore",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "client",
    srcs = [
        "client.cc",
    ],
    hdrs = [
        "client.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/http:request_builder",
        "//src/v/http:utils",
        "//src/v/ssx:future_util",
        "@boost//:beast",
    ],
    visibility = [
        "//src/v/cluster_link:__subpackages__",
        "//src/v/pandaproxy:__subpackages__",
    ],
    deps = [
        ":credentials",
        ":error",
        ":parse",
        ":retry_policy",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/http",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)
