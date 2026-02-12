load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "iotune",
    srcs = [
        "data.go",
        "io_tune.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/iotune",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/cloud/provider",
            "//src/go/rpk/pkg/osutil",
            "@org_uber_go_zap//:zap",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/cloud/provider",
            "//src/go/rpk/pkg/osutil",
            "@org_uber_go_zap//:zap",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "iotune_test",
    size = "small",
    srcs = ["data_test.go"],
    deps = select({
        "@rules_go//go/platform:android": [
            ":iotune",
            "@com_github_stretchr_testify//require",
        ],
        "@rules_go//go/platform:linux": [
            ":iotune",
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
