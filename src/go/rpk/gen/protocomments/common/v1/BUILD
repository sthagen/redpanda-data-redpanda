load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "common",
    srcs = ["comments.pb.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/gen/protocomments/common/v1",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_protobuf//reflect/protoreflect"],
)
