load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "leb128",
    hdrs = [
        "leb128.h",
    ],
    include_prefix = "wasm/parser",
    visibility = [
        "//src/v/wasm/parser:__pkg__",
        "//src/v/wasm/parser/tests:__pkg__",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
    ],
)

redpanda_cc_library(
    name = "parser",
    srcs = [
        "parser.cc",
    ],
    hdrs = [
        "parser.h",
    ],
    implementation_deps = [
        ":leb128",
        "//src/v/bytes",
        "//src/v/bytes:iobuf_parser",
        "//src/v/strings:utf8",
        "@fmt",
    ],
    include_prefix = "wasm/parser",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)
