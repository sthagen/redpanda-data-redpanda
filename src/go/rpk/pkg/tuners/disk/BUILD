load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "disk",
    srcs = [
        "block_device.go",
        "block_device_linux.go",
        "block_devices.go",
        "block_devices_linux.go",
        "device_features.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/osutil",
            "//src/go/rpk/pkg/rpkutil",
            "//src/go/rpk/pkg/system",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_spf13_afero//:afero",
            "@org_golang_x_sys//unix",
            "@org_uber_go_zap//:zap",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/osutil",
            "//src/go/rpk/pkg/rpkutil",
            "//src/go/rpk/pkg/system",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_spf13_afero//:afero",
            "@org_golang_x_sys//unix",
            "@org_uber_go_zap//:zap",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "disk_test",
    size = "small",
    srcs = [
        "block_device_test.go",
        "block_devices_test.go",
        "device_features_test.go",
    ],
    embed = [":disk"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/osutil",
            "//src/go/rpk/pkg/rpkutil",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_spf13_afero//:afero",
            "@com_github_stretchr_testify//require",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/osutil",
            "//src/go/rpk/pkg/rpkutil",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_spf13_afero//:afero",
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
