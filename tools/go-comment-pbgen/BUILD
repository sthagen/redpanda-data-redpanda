load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go-comment-pbgen_lib",
    srcs = [
        "generator.go",
        "main.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/tools/go-comment-pbgen",
    visibility = ["//visibility:private"],
    deps = [
        "@org_golang_google_protobuf//compiler/protogen",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/pluginpb",
    ],
)

go_binary(
    name = "go-comment-pbgen",
    embed = [":go-comment-pbgen_lib"],
    visibility = ["//visibility:public"],
)
