load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "network",
    srcs = [
        "const.go",
        "nic.go",
        "nics.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/rpkutil",
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_safchain_ethtool//:ethtool",
            "@com_github_spf13_afero//:afero",
            "@org_uber_go_zap//:zap",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/rpkutil",
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_safchain_ethtool//:ethtool",
            "@com_github_spf13_afero//:afero",
            "@org_uber_go_zap//:zap",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "network_test",
    size = "small",
    srcs = [
        "nic_test.go",
        "nics_test.go",
    ],
    embed = [":network"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_spf13_afero//:afero",
            "@com_github_stretchr_testify//require",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_spf13_afero//:afero",
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
