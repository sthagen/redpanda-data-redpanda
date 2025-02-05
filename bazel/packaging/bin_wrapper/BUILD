load("@rules_go//go:def.bzl", "go_binary")

# gazelle:ignore

go_binary(
    name = "rpk",
    srcs = ["main.go"],
    visibility = ["//bazel/packaging:__pkg__"],
    x_defs = {
        "binaryPath": "/opt/redpanda/libexec/rpk",
    },
)

go_binary(
    name = "redpanda",
    srcs = ["main.go"],
    visibility = ["//bazel/packaging:__pkg__"],
    x_defs = {
        "binaryPath": "/opt/redpanda/libexec/redpanda",
        "ldLibraryPath": "/opt/redpanda/lib",
    },
)

go_binary(
    name = "iotune",
    srcs = ["main.go"],
    visibility = ["//bazel/packaging:__pkg__"],
    x_defs = {
        "binaryPath": "/opt/redpanda/libexec/iotune",
        "ldLibraryPath": "/opt/redpanda/lib",
    },
)
