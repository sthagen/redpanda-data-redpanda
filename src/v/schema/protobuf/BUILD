load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

filegroup(
    name = "google_type_proto_files",
    srcs = [
        "google/type/calendar_period.proto",
        "google/type/color.proto",
        "google/type/date.proto",
        "google/type/datetime.proto",
        "google/type/dayofweek.proto",
        "google/type/decimal.proto",
        "google/type/expr.proto",
        "google/type/fraction.proto",
        "google/type/interval.proto",
        "google/type/latlng.proto",
        "google/type/localized_text.proto",
        "google/type/money.proto",
        "google/type/month.proto",
        "google/type/phone_number.proto",
        "google/type/postal_address.proto",
        "google/type/quaternion.proto",
        "google/type/timeofday.proto",
    ],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "google_type_protos",
    srcs = [":google_type_proto_files"],
    strip_import_prefix = "/src/v/schema/protobuf/",
    visibility = ["//visibility:public"],
    deps = [
        "@protobuf//:duration_proto",
        "@protobuf//:timestamp_proto",
        "@protobuf//:wrappers_proto",
    ],
)

cc_proto_library(
    name = "google_type_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":google_type_protos"],
)

filegroup(
    name = "confluent_type_proto_files",
    srcs = [
        "confluent/meta.proto",
        "confluent/types/decimal.proto",
    ],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "confluent_type_protos",
    srcs = [
        ":confluent_type_proto_files",
    ],
    strip_import_prefix = "/src/v/schema/protobuf/",
    visibility = ["//visibility:public"],
    deps = [
        "@protobuf//:descriptor_proto",
    ],
)

cc_proto_library(
    name = "confluent_type_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":confluent_type_protos"],
)
