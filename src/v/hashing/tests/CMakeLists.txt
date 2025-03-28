rp_test(
  UNIT_TEST
  BINARY_NAME xx_hashes
  SOURCES xx_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::hashing
  LABELS hashing
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME murmur_hashes
  SOURCES murmur_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    v::gtest_main
    v::hashing
  LABELS hashing
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_secure_hashing
  SOURCES secure_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::hashing v::seastar_testing_main v::bytes
  LABELS hashing
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME hashing_bench
  SOURCES hash_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::hashing v::random v::test_random_bytes absl::hash v::bytes
  LABELS hashing
)
