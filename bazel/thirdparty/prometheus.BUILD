load("@bazel_skylib//rules:native_binary.bzl", "native_binary")

filegroup(
    name = "prometheus_bin",
    srcs = ["prom/prometheus"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "promtool",
    srcs = ["prom/promtool"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "prometheus_config",
    srcs = ["prom/prometheus.yml"],
    visibility = ["//visibility:public"],
)

native_binary(
    name = "prometheus",
    src = ":prometheus_bin",
    data = [
        ":prometheus_config",
    ],
    visibility = ["//visibility:public"],
)
