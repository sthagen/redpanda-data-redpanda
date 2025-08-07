load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/batch_cache:__pkg__",
    "//src/v/cloud_topics/core:__pkg__",
    "//src/v/cloud_topics/core/tests:__pkg__",
    "//src/v/cloud_topics/frontend:__pkg__",
    "//src/v/cloud_topics/level_zero:__subpackages__",
    "//src/v/cloud_topics/level_zero/throttler:__pkg__",
    "//src/v/cloud_topics/level_zero/throttler/tests:__pkg__",
    "//src/v/cloud_topics/tests:__pkg__",
    "//src/v/kafka/data:__pkg__",
])

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "errc.h",
        "types.h",
    ],
    include_prefix = "cloud_topics",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/serde",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "data_plane_api",
    hdrs = [
        "data_plane_api.h",
    ],
    include_prefix = "cloud_topics",
    visibility = [
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/level_zero/reader:__pkg__",
        "//src/v/cloud_topics/level_zero/reconciler:__pkg__",
        "//src/v/kafka/data:__pkg__",
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "app",
    srcs = [
        "app.cc",
    ],
    hdrs = [
        "app.h",
    ],
    implementation_deps = [
        ":cluster_services_interface",
        ":data_plane_impl",
    ],
    include_prefix = "cloud_topics",
    visibility = [
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/level_zero/reader:__pkg__",
        "//src/v/cloud_topics/level_zero/reconciler:__pkg__",
        "//src/v/kafka/data:__pkg__",
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics/level_one/domain:domain_supervisor",
        "//src/v/cloud_topics/level_one/metastore:frontend",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/reconciler",
        "//src/v/cluster",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:sharded_service_container",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_plane_impl",
    srcs = [
        "data_plane_impl.cc",
    ],
    hdrs = [
        "data_plane_impl.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage:cache",
        "//src/v/cloud_topics:cluster_services_interface",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cloud_topics/level_zero/batcher",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/cloud_topics/level_zero/reader:fetch_handler",
        "//src/v/cloud_topics/level_zero/throttler",
        "//src/v/model",
        "//src/v/storage",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//src/v/redpanda:__pkg__"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "object_utils",
    srcs = [
        "object_utils.cc",
    ],
    hdrs = [
        "object_utils.h",
    ],
    include_prefix = "cloud_topics",
    deps = [
        ":cluster_services_interface",
        ":types",
        "//src/v/cloud_storage_clients",
        "//src/v/ssx:sformat",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "state_accessors",
    hdrs = [
        "state_accessors.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        ":data_plane_api",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cluster_services_interface",
    hdrs = [
        "cluster_services.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ephemeral_cluster_services",
    hdrs = [
        "ephemeral_cluster_services.h",
    ],
    include_prefix = "cloud_topics",
    visibility = ["//visibility:public"],
    deps = [
        ":cluster_services_interface",
        "@seastar",
    ],
)
