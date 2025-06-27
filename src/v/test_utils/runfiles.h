#pragma once

#include <string>
#include <string_view>

namespace test_utils {

/*
 * Usage:
 *  - Add a `data = [path/to/some.file]` to the test target
 *  - Call get_runfile_path with "root/.../path/to/some.file"
 *
 * The return value is the path to the file.
 */
std::string get_runfile_path(std::string_view);

} // namespace test_utils
