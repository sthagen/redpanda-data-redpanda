load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "publicapi",
    srcs = [
        "controlplane.go",
        "dataplane.go",
        "enterprise.go",
        "publicapi.go",
        "transform.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/httpapi",
        "@build_buf_gen_go_redpandadata_cloud_connectrpc_go//redpanda/api/controlplane/v1beta2/controlplanev1beta2connect",
        "@build_buf_gen_go_redpandadata_cloud_connectrpc_go//redpanda/api/iam/v1beta2/iamv1beta2connect",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/controlplane/v1beta2",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/iam/v1beta2",
        "@build_buf_gen_go_redpandadata_common_protocolbuffers_go//redpanda/api/common/v1alpha1",
        "@build_buf_gen_go_redpandadata_dataplane_connectrpc_go//redpanda/api/dataplane/v1alpha2/dataplanev1alpha2connect",
        "@build_buf_gen_go_redpandadata_dataplane_protocolbuffers_go//redpanda/api/dataplane/v1alpha2",
        "@build_buf_gen_go_redpandadata_gatekeeper_connectrpc_go//redpanda/api/gatekeeper/v1alpha1/gatekeeperv1alpha1connect",
        "@com_connectrpc_connect//:connect",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "publicapi_test",
    srcs = ["controlplane_test.go"],
    embed = [":publicapi"],
    deps = ["@com_github_stretchr_testify//require"],
)
