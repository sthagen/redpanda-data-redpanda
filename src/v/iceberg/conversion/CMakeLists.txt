find_package(Protobuf REQUIRED)

v_cc_library(
  NAME iceberg_conversion
  SRCS
    protobuf_utils.cc
    schema_avro.cc
    schema_parquet.cc
    schema_protobuf.cc
    values_avro.cc
    values_parquet.cc
    values_protobuf.cc
  DEPS
    v::iceberg
    Seastar::seastar
    protobuf::libprotobuf
    v::serde_avro
)

add_subdirectory(tests)
