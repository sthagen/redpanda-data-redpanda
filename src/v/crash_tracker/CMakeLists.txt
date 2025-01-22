v_cc_library(
  NAME crash_tracker
  SRCS
    limiter.cc
    logger.cc
    recorder.cc
    service.cc
  DEPS
    Seastar::seastar
    v::base
    v::bytes
    v::model
    v::serde
    v::hashing
    v::random
)
