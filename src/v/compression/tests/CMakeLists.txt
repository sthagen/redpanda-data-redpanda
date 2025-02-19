rp_test(
  BENCHMARK_TEST
  BINARY_NAME zstd_stream
  SOURCES zstd_stream_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::compression v::random
  LABELS compression
)
rp_test(
  UNIT_TEST
  BINARY_NAME zstd_tests
  SOURCES zstd_tests.cc
  LIBRARIES v::seastar_testing_main v::compression v::random
  LABELS compression
  ARGS "-- -c 1"
  )

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME lz4_buf_tests
  SOURCES lz4_buf_tests.cc
  LIBRARIES v::compression v::gtest_main
  LABELS compression
  ARGS "-- -c 1"
)


rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gzip_stream_decompression_tests
  SOURCES gzip_stream_decompression_tests.cc
  LIBRARIES v::compression v::gtest_main v::utils
  LABELS compression
  ARGS "-- -c 1"
)

set(SNAPPY_PAYLOAD_PATH "${CMAKE_CURRENT_SOURCE_DIR}/snappy_payload")

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME snappy_tests
  SOURCES snappy_tests.cc
  LIBRARIES v::compression v::random v::gtest_main v::utils
  LABELS compression
  ARGS "-- -c 1"
  INPUT_FILES
   "${SNAPPY_PAYLOAD_PATH}/uncompressed_data"
   "${SNAPPY_PAYLOAD_PATH}/little_endian_compressed_data.snappy"
  ENV "SNAPPY_PAYLOAD_PATH="
  )
