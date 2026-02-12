load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "hwloc",
    srcs = [
        "cpuset.go",
        "hwloc.go",
        "hwloc_cmd.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/osutil",
            "@org_uber_go_zap//:zap",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/osutil",
            "@org_uber_go_zap//:zap",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "hwloc_test",
    size = "small",
    srcs = ["cpuset_test.go"],
    embed = [":hwloc"],
    deps = select({
        "@rules_go//go/platform:android": [
            "@com_github_stretchr_testify//require",
        ],
        "@rules_go//go/platform:linux": [
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
