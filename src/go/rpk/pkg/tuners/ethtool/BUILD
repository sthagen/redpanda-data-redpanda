load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "ethtool",
    srcs = ["ethtool.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:linux": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "//conditions:default": [],
    }),
)
