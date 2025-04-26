# gazelle:ignore
# This file depends on a build tag and gazelle tries to edit this file.
load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "fips",
    srcs = [
        "disabled.go",
        "enabled.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/fips",
    visibility = ["//visibility:public"],
)
