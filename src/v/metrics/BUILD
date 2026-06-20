load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "metrics",
    srcs = [
        "host_metrics_watcher.cc",
        "metrics.cc",
        "metrics_registry.cc",
    ],
    hdrs = [
        "aggregate_metrics_watcher.h",
        "host_metrics_watcher.h",
        "metrics.h",
        "metrics_registry.h",
        "prometheus_sanitize.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/utils:device_utils",
        "//src/v/utils:file_io",
        "//src/v/utils:hdr_hist",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "instance_info",
    srcs = [
        "instance_info.cc",
        "instance_info_table.cc",
    ],
    hdrs = [
        "instance_info.h",
        "instance_info_impl.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

# Private to the instance_type_detector library, but its unit test compiles it
# directly to exercise query_ec2_instance_type().
exports_files(
    ["instance_type_detector_impl.h"],
    visibility = ["//src/v/metrics/tests:__pkg__"],
)

redpanda_cc_library(
    name = "instance_type_detector",
    srcs = [
        "instance_type_detector.cc",
        "instance_type_detector_impl.h",
    ],
    hdrs = [
        "instance_type_detector.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_instance_metadata",
        "//src/v/http",
        "//src/v/net",
        "//src/v/ssx:future_util",
        "//src/v/utils:unresolved_address",
        "@boost//:beast",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":instance_info",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "instance_metrics",
    srcs = [
        "instance_metrics.cc",
    ],
    hdrs = [
        "instance_metrics.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":instance_type_detector",
        ":metrics",
        "//src/v/base",
        "@seastar",
    ],
)
