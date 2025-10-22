load("@bazel_skylib//rules:native_binary.bzl", "native_binary")

filegroup(
    name = "grafana_server",
    srcs = ["bin/grafana-server"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "grafana_cli",
    srcs = ["bin/grafana-cli"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "grafana_files",
    srcs = glob([
        "conf/**",
        "public/**",
    ]),
    visibility = ["//visibility:public"],
)

native_binary(
    name = "grafana",
    src = ":grafana_server",
    data = [
        ":grafana_files",
    ],
    visibility = ["//visibility:public"],
)
