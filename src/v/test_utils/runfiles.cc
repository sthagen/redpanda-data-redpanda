#include "test_utils/runfiles.h"

#include "tools/cpp/runfiles/runfiles.h"

#include <fmt/format.h>

#include <cstdlib>
#include <filesystem>
#include <memory>

namespace test_utils {

std::string get_runfile_path(std::string_view path) {
    using bazel::tools::cpp::runfiles::Runfiles;
    std::string error;
    std::unique_ptr<Runfiles> runfiles;
    if (std::getenv("BAZEL_TEST") != nullptr) {
        runfiles.reset(
          Runfiles::CreateForTest(BAZEL_CURRENT_REPOSITORY, &error));
    } else {
        static auto argv0 = std::filesystem::canonical("/proc/self/exe");
        runfiles.reset(
          Runfiles::Create(argv0, BAZEL_CURRENT_REPOSITORY, &error));
    }
    if (runfiles == nullptr) {
        throw std::runtime_error(error);
    }
    return runfiles->Rlocation(fmt::format("_main/{}", path));
}

} // namespace test_utils
