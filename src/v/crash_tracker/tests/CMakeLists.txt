rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME limiter_test
  SOURCES
    limiter_test.cc
  LIBRARIES v::gtest_main v::crash_tracker Seastar::seastar
  ARGS "-- -c 1"
)

add_executable(crash_report_generator report_generator.cc)
set_property(TARGET crash_report_generator PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(crash_report_generator PUBLIC v::crash_tracker)
install(TARGETS crash_report_generator DESTINATION bin)
