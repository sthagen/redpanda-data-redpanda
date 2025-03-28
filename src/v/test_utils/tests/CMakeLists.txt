rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_test
  SOURCES gtest_test.cc
  LIBRARIES v::gtest_main)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_listener
  SOURCES
    gtest_listener_test.cc
  LIBRARIES v_gtest_utils GTest::gtest
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME test_random_bytes_test
  SOURCES random_bytes_test.cc
  LIBRARIES v::gtest_main v::test_random_bytes
)
