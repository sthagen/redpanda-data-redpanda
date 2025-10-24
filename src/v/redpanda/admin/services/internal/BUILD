load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/redpanda:__subpackages__",
])

redpanda_cc_library(
    name = "debug",
    srcs = ["debug.cc"],
    hdrs = ["debug.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/v1:debug_redpanda_proto",
        "//src/v/base",
        "//src/v/finjector",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:to_string",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "breakglass",
    srcs = ["breakglass.cc"],
    hdrs = ["breakglass.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/v1:breakglass_redpanda_proto",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/finjector",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:to_string",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "metastore",
    srcs = ["metastore.cc"],
    hdrs = ["metastore.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/cloud_topics/v1:metastore_redpanda_proto",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cluster",
        "//src/v/serde/protobuf:rpc",
        "@fmt",
        "@seastar",
    ],
)
