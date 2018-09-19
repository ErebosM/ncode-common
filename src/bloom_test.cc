#include "bloom.h"

#include <cstring>
#include "gtest/gtest.h"

namespace nc {
namespace {

static constexpr size_t kMillion = 1000000UL;

struct SomeCustomDatatype {
  int value;
  int other;
  double double_value;
};

TEST(Bloom, BadSize) {
  ASSERT_DEATH(BloomFilter<SomeCustomDatatype> filter(0), ".*");
}

TEST(Bloom, SimpleEmpty) {
  BloomFilter<SomeCustomDatatype> filter(kMillion);
  ASSERT_FALSE(filter.PossiblyContains({1, 2, 4.0}));
}

TEST(Bloom, Simple) {
  BloomFilter<SomeCustomDatatype> filter(kMillion);
  filter.Add({1, 2, 3.0});
  ASSERT_TRUE(filter.PossiblyContains({1, 2, 3.0}));
  ASSERT_FALSE(filter.PossiblyContains({1, 2, 4.0}));
}

TEST(Bloom, Saturated) {
  BloomFilter<SomeCustomDatatype> filter(1);
  for (int i = 0; i < 10; ++i) {
    filter.Add({i, i, 3.0});
  }
  ASSERT_EQ(1.0, filter.SaturationFactor());

  ASSERT_TRUE(filter.PossiblyContains({1, 1, 3.0}));
  ASSERT_TRUE(filter.PossiblyContains({1, 2, 4.0}));
}

TEST(Bloom, LotsOfValues) {
  BloomFilter<SomeCustomDatatype> filter(kMillion);
  for (int i = 0; i < static_cast<int>(kMillion); ++i) {
    filter.Add({i, i, static_cast<double>(i)});
  }

  for (int i = 0; i < static_cast<int>(kMillion); ++i) {
    ASSERT_TRUE(filter.PossiblyContains({i, i, static_cast<double>(i)}));
  }
}

TEST(Bloom, Strings) {
  BloomFilter<std::string> filter(kMillion);

  std::string string_one =
      "some long string that will not fit in the small-strings cache of "
      "std::string";
  std::string string_two = string_one;

  filter.Add(string_one);
  ASSERT_TRUE(filter.PossiblyContains(string_two));
}

TEST(Bloom, Vectors) {
  BloomFilter<std::vector<int>> filter(kMillion);

  std::vector<int> v1 = {1,2,3};
  std::vector<int> v2 = {1,2,3};

  filter.Add(v1);
  ASSERT_TRUE(filter.PossiblyContains(v2));
}


}  // namespace
}  // namespace nc
