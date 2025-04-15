find_package(Avro REQUIRED)
set(testdata_dir "${CMAKE_CURRENT_SOURCE_DIR}/testdata")

set(PROTO_FILES
  testdata/person.proto
  testdata/complex.proto
  testdata/iceberg_ready_test_messages_edition2023.proto
  testdata/not_supported.proto
  testdata/proto2.proto
)

set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})


v_cc_library(
  NAME iceberg_test_proto_cc_files
  SRCS ${PROTO_SRCS}
  DEPS protobuf::libprotobuf
)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME iceberg_protobuf
  SOURCES 
  iceberg_protobuf_tests.cc
  LIBRARIES
  v::gtest_main
  v::datalake_writer
  v::iceberg_test_utils
  v::iceberg_test_proto_cc_files
  v::iceberg_conversion
  INPUT_FILES
  "${testdata_dir}/iceberg_ready_test_messages_edition2023.proto"
  LABELS iceberg
  ARGS "-- -c 1"
)

target_include_directories(v_iceberg_test_proto_cc_files
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME iceberg_avro
  SOURCES 
  iceberg_avro_tests.cc
  LIBRARIES
  v::gtest_main
  v::datalake_writer
  v::iceberg_test_utils
  v::avro_test_utils
  v::iceberg_conversion
  LABELS iceberg
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME iceberg_parquet
  SOURCES
  iceberg_parquet_tests.cc
  LIBRARIES
  v::gtest_main
  v::datalake_writer
  v::iceberg_test_utils
  v::avro_test_utils
  v::iceberg_conversion
  LABELS iceberg
  ARGS "-- -c 1"
)
