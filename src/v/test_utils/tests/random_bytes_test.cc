#include "test_utils/random_bytes.h"

#include <gtest/gtest.h>

#include <iterator>

TEST(RandomBytesTest, SplitEmptyString) {
    int n_fragments = 10;
    auto result = tests::fragmented_iobuf(std::string{}, n_fragments);

    ASSERT_EQ(result.size_bytes(), 0);
    ASSERT_EQ(std::ranges::distance(result), n_fragments);
    for (const auto& fragment : result) {
        ASSERT_EQ(fragment.size(), 0);
    }
}
