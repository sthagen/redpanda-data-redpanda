#pragma once

#include <clang-tidy/ClangTidyCheck.h>

namespace clang::tidy::redpanda {

/**
 * A clang-tidy check that does nothing.
 *
 * Included for demonstration purposes and so the tool doesn't complain about
 * the 'redpanda-*' glob in .clang-tidy.
 *
 */
class NoopCheck : public ClangTidyCheck {
public:
    NoopCheck(StringRef Name, ClangTidyContext* Context)
      : ClangTidyCheck(Name, Context) {}
};

} // namespace clang::tidy::redpanda
