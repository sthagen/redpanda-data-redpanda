load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "topic_properties_utils",
    srcs = [
        "topic_properties_utils.cc",
    ],
    hdrs = [
        "topic_properties_utils.h",
    ],
    visibility = ["//src/v/cluster_link:__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/kafka/server:topic_config_utils",
        "//src/v/reflection:type_traits",
    ],
)
