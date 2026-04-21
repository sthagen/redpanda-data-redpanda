#pragma once

#include <string>
#include <string_view>

namespace test_utils {

/*
 * Resolve `path` to an absolute filesystem path at test runtime.
 *
 * `path` is a repo-relative path (e.g. "src/v/.../testdata/foo.json" for
 * files from this repository). The file must be listed in the test
 * target's `data = [...]` attribute so Bazel stages it into the
 * runfiles tree.
 *
 * `repo` selects which Bazel repository the path is rooted in. It
 * defaults to "_main" (this repository). For files exposed by an
 * external repository — e.g. a `http_archive` declared in MODULE.bazel —
 * pass that repository's name.
 */
std::string
get_runfile_path(std::string_view path, std::string_view repo = "_main");

} // namespace test_utils
