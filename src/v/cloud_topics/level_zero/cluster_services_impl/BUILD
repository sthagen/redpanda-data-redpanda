load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    ":__subpackages__",
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero:__pkg__",
    "//src/v/cloud_topics/level_zero/batcher:__pkg__",
])

redpanda_cc_library(
    name = "cluster_services_impl",
    hdrs = [
        "cluster_services.h",
    ],
    include_prefix = "cloud_topics/level_zero/cluster_services_impl",
    deps = [
        "//src/v/cloud_topics:cluster_services_interface",
        "//src/v/cluster",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
