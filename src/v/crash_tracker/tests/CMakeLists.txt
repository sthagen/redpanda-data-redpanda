rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME limiter_test
  SOURCES
    limiter_test.cc
  LIBRARIES v::gtest_main v::crash_tracker v::version v::utils Seastar::seastar
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME recorder_test
  SOURCES
    recorder_test.cc
    utils.h
  LIBRARIES v::gtest_main v::config v::crash_tracker Seastar::seastar
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME crash_recorder_types_test
  SOURCES
    types_test.cc
  LIBRARIES v::gtest_main v::crash_tracker
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME crash_reporter_oom_test
  SOURCES
    crash_reporter_oom_test.cc
    utils.h
  LIBRARIES
    v::config
    v::crash_tracker
    v::gtest_main
    v::resource_mgmt
    Seastar::seastar
)

add_executable(crash_report_generator report_generator.cc)
set_property(TARGET crash_report_generator PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(crash_report_generator PUBLIC v::crash_tracker)
install(TARGETS crash_report_generator DESTINATION bin)
