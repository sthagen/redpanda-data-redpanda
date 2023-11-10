include(FetchContent)

set(FETCHCONTENT_QUIET FALSE)

# don't require that cache entries be created for crc32c options so we can use
# normal variables via set(). a better solution here would be to go update the
# crc32c cmake build.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

function(fetch_dep NAME)
  cmake_parse_arguments(fetch_dep_args "" "REPO;TAG" "" ${ARGN})
  FetchContent_Declare(
    ${NAME}
    GIT_REPOSITORY ${fetch_dep_args_REPO}
    GIT_TAG ${fetch_dep_args_TAG}
    GIT_SHALLOW ON
    GIT_SUBMODULES ""
    GIT_PROGRESS TRUE
    USES_TERMINAL_DOWNLOAD TRUE
    OVERRIDE_FIND_PACKAGE
    SYSTEM
    ${fetch_dep_args_UNPARSED_ARGUMENTS})
endfunction()

fetch_dep(fmt
  REPO https://github.com/fmtlib/fmt.git
  TAG 8.1.1)

# CMakeLists.txt is patched to avoid registering tests. We still want the
# Seastar testing library to be built, but we don't want the tests to run. This
# could be accomplished with Seastar_INSTALL=ON, but this doesn't play nice with
# the add_subdirectory method of using Seastar.
set(Seastar_TESTING ON CACHE BOOL "" FORCE)
set(Seastar_API_LEVEL 6 CACHE STRING "" FORCE)
set(Seastar_CXX_FLAGS -Wno-error)
fetch_dep(seastar
  REPO https://github.com/redpanda-data/seastar.git
  TAG v23.3.x
  PATCH_COMMAND sed -i "s/add_subdirectory (tests/# add_subdirectory (tests/g" CMakeLists.txt)

fetch_dep(avro
  REPO https://github.com/redpanda-data/avro
  TAG release-1.11.1-redpanda
  SOURCE_SUBDIR redpanda_build)

fetch_dep(rapidjson
  REPO https://github.com/redpanda-data/rapidjson.git
  TAG 14a5dd756e9bef26f9b53d3b4eb1b73c6a1794d5
  SOURCE_SUBDIR redpanda_build)

set(CRC32C_BUILD_TESTS OFF)
set(CRC32C_BUILD_BENCHMARKS OFF)
set(CRC32C_USE_GLOG OFF)
set(CRC32C_INSTALL OFF)
fetch_dep(crc32c
  REPO https://github.com/google/crc32c.git
  TAG 1.1.2)

set(BASE64_BUILD_CLI OFF)
set(BASE64_BUILD_TESTS OFF)
fetch_dep(base64
  REPO https://github.com/aklomp/base64.git
  TAG v0.5.0)

fetch_dep(roaring
  REPO https://github.com/redpanda-data/CRoaring.git
  TAG redpanda
  SOURCE_SUBDIR redpanda_build)

fetch_dep(GTest
  REPO https://github.com/google/googletest
  TAG v1.14.0)

fetch_dep(ada
  REPO https://github.com/ada-url/ada
  TAG v2.7.2)

if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64")
  set(TINYGO_TARBALL "tinygo-linux-amd64.tar.gz")
  set(TINYGO_MD5 "5b4ba9d8dd8ce10f7939de441ad46a80")
elseif(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
  set(TINYGO_TARBALL "tinygo-linux-arm64.tar.gz")
  set(TINYGO_MD5 "a0b397306665a9634a6b9c716f60f0a6")
endif()

FetchContent_Declare(tinygo
  URL https://github.com/redpanda-data/tinygo/releases/download/v0.30.0-rpk1/${TINYGO_TARBALL}
  URL_HASH MD5=${TINYGO_MD5}
  DOWNLOAD_EXTRACT_TIMESTAMP ON)
FetchContent_GetProperties(tinygo)

fetch_dep(hdrhistogram
  REPO https://github.com/HdrHistogram/HdrHistogram_c
  TAG 0.11.5)

list(APPEND WASMTIME_USER_CARGO_BUILD_OPTIONS --no-default-features)
list(APPEND WASMTIME_USER_CARGO_BUILD_OPTIONS --features=async)
list(APPEND WASMTIME_USER_CARGO_BUILD_OPTIONS --features=addr2line)

# We need submodules for wasmtime to compile
FetchContent_Declare(
  wasmtime
  GIT_REPOSITORY https://github.com/bytecodealliance/wasmtime
  GIT_TAG 282edac149c0883a7d064132f93ce0a20a50d0d7
  GIT_PROGRESS TRUE
  USES_TERMINAL_DOWNLOAD TRUE
  OVERRIDE_FIND_PACKAGE
  SYSTEM
  SOURCE_SUBDIR crates/c-api)

set(ABSL_PROPAGATE_CXX_STD ON)
fetch_dep(absl
  REPO https://github.com/abseil/abseil-cpp
  TAG 20220623.0)

FetchContent_MakeAvailable(
    fmt
    rapidjson
    seastar
    GTest
    crc32c
    base64
    roaring
    avro
    tinygo
    wasmtime
    hdrhistogram
    ada
    absl)

add_library(Crc32c::crc32c ALIAS crc32c)
add_library(aklomp::base64 ALIAS base64)
add_library(Hdrhistogram::hdr_histogram ALIAS hdr_histogram)

list(APPEND CMAKE_PROGRAM_PATH ${tinygo_SOURCE_DIR}/bin)
