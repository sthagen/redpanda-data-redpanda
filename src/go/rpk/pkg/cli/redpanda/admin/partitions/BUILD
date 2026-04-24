load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "partitions",
    srcs = ["partitions.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/admin/partitions",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/kafka",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
    ],
)

go_test(
    name = "partitions_test",
    srcs = ["partitions_test.go"],
    embed = [":partitions"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_stretchr_testify//require",
    ],
)
