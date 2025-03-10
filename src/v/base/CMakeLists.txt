v_cc_library(
    NAME base
    SRCS
      vassert.cc
    DEPS
      Seastar::seastar
)
add_subdirectory(tests)
