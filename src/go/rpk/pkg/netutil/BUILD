load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "netutil",
    srcs = [
        "hostport.go",
        "interfaces.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/netutil",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/rpkutil",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "netutil_test",
    srcs = ["hostport_test.go"],
    embed = [":netutil"],
    deps = ["@com_github_stretchr_testify//require"],
)
