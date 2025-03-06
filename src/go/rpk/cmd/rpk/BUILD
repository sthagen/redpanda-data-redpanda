load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "rpk_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/cmd/rpk",
    visibility = ["//visibility:private"],
    deps = ["//src/go/rpk/pkg/cli"],
)

go_binary(
    name = "rpk",
    embed = [":rpk_lib"],
    visibility = ["//visibility:public"],
    x_defs = {
        "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version.version": "{STABLE_GIT_LATEST_TAG}",
        "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version.rev": "{STABLE_GIT_COMMIT}",
        "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version.buildTime": "{FORMATTED_DATE}",
        "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common.tag": "{STABLE_GIT_LATEST_TAG}",
    },
)
