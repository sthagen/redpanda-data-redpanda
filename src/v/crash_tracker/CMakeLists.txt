v_cc_library(
  NAME crash_tracker
  SRCS
    limiter.cc
    logger.cc
    prepared_writer.cc
    recorder.cc
    service.cc
    signals.cc
    types.cc
  DEPS
    Seastar::seastar
    v::config
    v::base
    v::bytes
    v::model
    v::serde
    v::hashing
    v::random
    v::version
)

add_subdirectory(tests)
