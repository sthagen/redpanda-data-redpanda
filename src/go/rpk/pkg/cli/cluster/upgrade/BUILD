load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "upgrade",
    srcs = [
        "finalize.go",
        "status.go",
        "upgrade.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/upgrade",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@com_connectrpc_connect//:connect",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)

go_test(
    name = "upgrade_test",
    srcs = ["status_test.go"],
    embed = [":upgrade"],
    deps = [
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@com_github_stretchr_testify//require",
    ],
)
