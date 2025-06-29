load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__subpackages__",
])

redpanda_cc_library(
    name = "level_one_object",
    srcs = [
        "level_one_object.cc",
    ],
    hdrs = [
        "level_one_object.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/reflection:type_traits",
        "//src/v/serde:vector",
        "//src/v/storage:record_batch_builder",
    ],
    include_prefix = "cloud_topics/objects",
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
        "@seastar",
    ],
)
