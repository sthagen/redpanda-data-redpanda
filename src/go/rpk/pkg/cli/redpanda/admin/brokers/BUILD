load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "brokers",
    srcs = [
        "brokers.go",
        "decommission.go",
        "decommission-status.go",
        "list.go",
        "recommission.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/admin/brokers",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/cluster",
        "//src/go/rpk/pkg/cli/cluster/brokers",
        "//src/go/rpk/pkg/cobraext",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
