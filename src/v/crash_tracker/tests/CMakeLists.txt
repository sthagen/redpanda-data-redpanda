rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME limiter_test
  SOURCES
    limiter_test.cc
  LIBRARIES v::gtest_main v::crash_tracker Seastar::seastar
  ARGS "-- -c 1"
)
