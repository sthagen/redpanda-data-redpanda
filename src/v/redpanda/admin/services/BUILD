load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/redpanda/admin:__subpackages__"])

redpanda_cc_library(
    name = "admin",
    srcs = ["admin.cc"],
    hdrs = ["admin.h"],
    include_prefix = "redpanda/admin/services",
    deps = [
        "//proto/redpanda/core/admin:admin_redpanda_proto",
        "//src/v/version",
    ],
)
