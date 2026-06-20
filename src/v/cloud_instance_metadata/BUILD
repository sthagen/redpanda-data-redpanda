load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "cloud_instance_metadata",
    srcs = [
        "aws_imds.cc",
        "azure_imds.cc",
        "gcp_metadata.cc",
    ],
    hdrs = [
        "aws_imds.h",
        "azure_imds.h",
        "gcp_metadata.h",
    ],
    implementation_deps = [
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:beast",
    ],
)
