load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "commands",
    srcs = [
        "backup_file.go",
        "command.go",
        "ethtool_change.go",
        "ethtool_set_channel.go",
        "execute.go",
        "sysctl_set.go",
        "systemd_install_unit.go",
        "systemd_start_unit.go",
        "write_file.go",
        "write_file_lines.go",
        "write_sized_file.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/osutil",
            "//src/go/rpk/pkg/rpkutil",
            "//src/go/rpk/pkg/system/systemd",
            "//src/go/rpk/pkg/tuners/ethtool",
            "@com_github_lorenzosaino_go_sysctl//:go-sysctl",
            "@com_github_safchain_ethtool//:ethtool",
            "@com_github_spf13_afero//:afero",
            "@org_golang_x_sys//unix",
            "@org_uber_go_zap//:zap",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/osutil",
            "//src/go/rpk/pkg/rpkutil",
            "//src/go/rpk/pkg/system/systemd",
            "//src/go/rpk/pkg/tuners/ethtool",
            "@com_github_lorenzosaino_go_sysctl//:go-sysctl",
            "@com_github_safchain_ethtool//:ethtool",
            "@com_github_spf13_afero//:afero",
            "@org_golang_x_sys//unix",
            "@org_uber_go_zap//:zap",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "commands_test",
    size = "small",
    srcs = [
        "systemd_install_unit_test.go",
        "systemd_start_unit_test.go",
        "write_file_test.go",
        "write_sized_file_test.go",
    ],
    deps = select({
        "@rules_go//go/platform:android": [
            ":commands",
            "//src/go/rpk/pkg/system/systemd",
            "@com_github_spf13_afero//:afero",
            "@com_github_stretchr_testify//require",
        ],
        "@rules_go//go/platform:linux": [
            ":commands",
            "//src/go/rpk/pkg/system/systemd",
            "@com_github_spf13_afero//:afero",
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
