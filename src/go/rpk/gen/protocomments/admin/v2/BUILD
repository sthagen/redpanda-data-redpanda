load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "admin",
    srcs = ["comments.pb.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/gen/protocomments/admin/v2",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_protobuf//reflect/protoreflect"],
)
