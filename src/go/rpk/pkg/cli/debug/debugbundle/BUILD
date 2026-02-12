load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "debugbundle",
    srcs = ["common.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/debugbundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/httpapi",
        "@com_github_spf13_pflag//:pflag",
    ],
)

go_test(
    name = "debugbundle_test",
    srcs = ["common_test.go"],
    embed = [":debugbundle"],
    deps = ["@com_github_stretchr_testify//require"],
)
