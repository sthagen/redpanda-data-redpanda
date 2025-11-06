load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "shadow_link_internal_proto",
    srcs = ["shadow_link_internal.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/admin/v2:shadow_link_proto",
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "shadow_link_internal_redpanda_proto",
    protos = [":shadow_link_internal_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/admin/v2:shadow_link_redpanda_proto",
    ],
)
