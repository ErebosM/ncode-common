#include "num_col.h"
#include "gtest/gtest.h"

#include <random>
#include "packer.h"

namespace nc {
namespace {

TEST(RangeSet, Init) {
  RangeSet<> set(std::vector<Range<>>({}));
  ASSERT_TRUE(set.ranges().empty());
}

TEST(RangeSet, Single) {
  RangeSet<> set(std::vector<Range<>>({{10, 15}}));

  std::vector<Range<>> ranges = {{10, 15}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, SingleZeroSize) {
  RangeSet<> set(std::vector<Range<>>({{10, 0}}));

  std::vector<Range<>> ranges = {};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, Sorted) {
  RangeSet<> set({{10, 15}, {5, 2}});

  std::vector<Range<>> ranges = {{5, 2}, {10, 15}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, MergeTwo) {
  RangeSet<> set({{10, 15}, {5, 5}});

  std::vector<Range<>> ranges = {{5, 20}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, MergeZeroSize) {
  RangeSet<> set({{10, 0}, {10, 0}, {10, 0}});

  std::vector<Range<>> ranges = {};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, Duplicates) {
  RangeSet<> set({{10, 1}, {10, 1}, {10, 1}, {100, 0}});

  std::vector<Range<>> ranges = {{10, 1}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, MergeSingle) {
  RangeSet<> set({{10, 1}, {11, 1}, {12, 1}});

  std::vector<Range<>> ranges = {{10, 3}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, OverlappingMerge) {
  RangeSet<> set({{10, 15}, {5, 10}});

  std::vector<Range<>> ranges = {{5, 20}};
  ASSERT_EQ(ranges, set.ranges());
}

template <typename T>
class ContainerTest : public ::testing::Test {};

using ContainerTypes =
    ::testing::Types<ImmutablePackedIntVector, RLEField<int64_t>>;
TYPED_TEST_CASE(ContainerTest, ContainerTypes);

TYPED_TEST(ContainerTest, Empty) {
  std::vector<int64_t> empty;
  TypeParam v(empty);
  ASSERT_EQ(0ul, v.size());
}

TYPED_TEST(ContainerTest, SingleValue) {
  TypeParam v({42});
  ASSERT_EQ(1ul, v.size());
  ASSERT_EQ(42, v.at(0));
}

TYPED_TEST(ContainerTest, SingleValueNegative) {
  TypeParam v({-42});
  ASSERT_EQ(1ul, v.size());
  ASSERT_EQ(-42, v.at(0));
}

TYPED_TEST(ContainerTest, SameValues) {
  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(1000000 + 42);
  }

  TypeParam v(values);
  ASSERT_EQ(1000000ul, v.size());

  for (size_t i = 0; i < 1000000; ++i) {
    ASSERT_EQ(1000000 + 42, v.at(i));
  }
}

TYPED_TEST(ContainerTest, Values) {
  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(i);
  }

  TypeParam v(values);
  ASSERT_EQ(1000000ul, v.size());

  for (int i = 0; i < 1000000; ++i) {
    ASSERT_EQ(i, v.at(i));
  }
}

TYPED_TEST(ContainerTest, ValuesReverse) {
  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(1000000 - i);
  }

  TypeParam v(values);
  ASSERT_EQ(1000000ul, v.size());

  for (int i = 0; i < 1000000; ++i) {
    ASSERT_EQ(1000000 - i, v.at(i));
  }
}

TYPED_TEST(ContainerTest, RandomValues) {
  std::mt19937 rnd(1);
  std::uniform_int_distribution<int64_t> dist;

  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(dist(rnd));
  }

  TypeParam v(values);
  ASSERT_EQ(1000000ul, v.size());

  for (size_t i = 0; i < 1000000; ++i) {
    ASSERT_EQ(values[i], v.at(i));
  }
}

TEST(SortedSubsequence, Empty) {
  std::vector<int64_t> values;
  ImmutablePackedIntVector v(values);

  ASSERT_TRUE(SortedSubsequence<>::Get(v).empty());
}

TEST(SortedSubsequence, SingleValue) {
  ImmutablePackedIntVector v({10});

  std::vector<SortedSubsequence<>> ss_model = {{{0, 1}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<>::Get(v));
}

TEST(SortedSubsequence, Increasing) {
  ImmutablePackedIntVector v({10, 11, 12, 13, 14, 15, 16});

  std::vector<SortedSubsequence<>> ss_model = {{{0, 7}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<>::Get(v));
}

TEST(SortedSubsequence, Decreasing) {
  ImmutablePackedIntVector v({16, 15, 14, 13, 12, 11, 10});

  std::vector<SortedSubsequence<>> ss_model = {{{0, 7}, false}};
  ASSERT_EQ(ss_model, SortedSubsequence<>::Get(v));
}

TEST(SortedSubsequence, IncreasingIncreasing) {
  ImmutablePackedIntVector v({10, 11, 5});

  std::vector<SortedSubsequence<>> ss_model = {{{0, 2}, true}, {{2, 1}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<>::Get(v));
}

TEST(SortedSubsequence, IncreasingIncreasingTwo) {
  ImmutablePackedIntVector v({10, 11, 5, 50});

  std::vector<SortedSubsequence<>> ss_model = {{{0, 2}, true}, {{2, 2}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<>::Get(v));
}

TEST(SortedSubsequence, DecreasingIncreasingIncreasing) {
  ImmutablePackedIntVector v({10, 9, 8, 50, 60, 80, 1, 2, 3});

  std::vector<SortedSubsequence<>> ss_model = {
      {{0, 3}, false}, {{3, 3}, true}, {{6, 3}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<>::Get(v));
}

TEST(SortedSubsequence, IncreasingRepeated) {
  ImmutablePackedIntVector v({10, 10, 10, 10, 11, 5, 5});

  std::vector<SortedSubsequence<>> ss_model = {{{0, 5}, true}, {{5, 2}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<>::Get(v));
}

TEST(SortedSubsequence, LotsOfValues) {
  std::mt19937 rnd(1);
  std::uniform_int_distribution<int64_t> dist;

  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(dist(rnd));
  }

  ImmutablePackedIntVector v(values);

  const std::vector<SortedSubsequence<>>& ss = SortedSubsequence<>::Get(v);
  LOG(INFO) << ss.size();
  LOG(INFO) << sizeof(SortedSubsequence<>) << " " << sizeof(Range<>);

  // std::vector<SortedSubsequence> ss_model = {
  //     {{2, 1}, false}, {{0, 2}, true}, {{3, 1}, true}};
  // ASSERT_EQ(ss_model, ss);
}

TEST(SortedSubsequence, BadRange) {
  ASSERT_DEATH(SortedSubsequence<> ss({0, 0}, true), ".*");
}

TEST(Bound, Increasing) {
  ImmutablePackedIntVector v({0, 1, 2, 50, 50, 80, 100, 101});
  SortedSubsequence<> ss({0, 8}, true);

  ASSERT_EQ(0ul, ss.Bound(v, 0));
  ASSERT_EQ(1ul, ss.Bound(v, 1));
  ASSERT_EQ(2ul, ss.Bound(v, 2));
  ASSERT_EQ(3ul, ss.Bound(v, 50));
  ASSERT_EQ(5ul, ss.Bound(v, 80));
  ASSERT_EQ(6ul, ss.Bound(v, 90));
  ASSERT_EQ(7ul, ss.Bound(v, 101));
  ASSERT_EQ(3ul, ss.Bound(v, 45));
}

TEST(Bound, Decreasing) {
  ImmutablePackedIntVector v({101, 100, 80, 50, 50, 2, 1, 0});
  SortedSubsequence<> ss({0, 8}, false);

  ASSERT_EQ(7ul, ss.Bound(v, 0));
  ASSERT_EQ(6ul, ss.Bound(v, 1));
  ASSERT_EQ(5ul, ss.Bound(v, 2));
  ASSERT_EQ(4ul, ss.Bound(v, 50));
  ASSERT_EQ(2ul, ss.Bound(v, 80));
  ASSERT_EQ(1ul, ss.Bound(v, 90));
  ASSERT_EQ(0ul, ss.Bound(v, 101));
  ASSERT_EQ(4ul, ss.Bound(v, 45));
}

TEST(UpperBound, Increasing) {
  ImmutablePackedIntVector v({0, 1, 2, 50, 50, 80, 100, 101});
  SortedSubsequence<> ss({0, 8}, true);

  ASSERT_EQ(8ul, ss.Bound(v, 101, false));
}

// TEST(ValuesInRange, DecreasingLarge) {
//   ImmutablePackedIntVector v({7692698082559361259L, 4064269471072392264L});
//   SortedSubsequence<int64_t> ss({0, 2}, false);
//
//   ASSERT_EQ(ss.IndicesOfValuesInRange(v, 7697914927906512855L,
//   1082636226378482417L), Range(0, 8));
// }

TEST(ValuesInRange, Increasing) {
  ImmutablePackedIntVector v({0, 1, 2, 50, 50, 80, 100, 101});
  SortedSubsequence<> ss({0, 8}, true);

  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 0, 101), Range<>(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 1, 101), Range<>(1, 7));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 2, 101), Range<>(2, 6));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 3, 101), Range<>(3, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 50, 101), Range<>(3, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 51, 101), Range<>(5, 3));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 80, 101), Range<>(5, 3));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 81, 101), Range<>(6, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 99, 101), Range<>(6, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 100, 101), Range<>(6, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 101, 101), Range<>(7, 1));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 1, 100), Range<>(1, 6));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 2, 100), Range<>(2, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 49, 100), Range<>(3, 4));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 51, 100), Range<>(5, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 50, 50), Range<>(3, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -1, 150), Range<>(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 0, 150), Range<>(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -10, 101), Range<>(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -10, -9), Range<>(0, 0));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 120, 150), Range<>(8, 0));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 100, 100), Range<>(6, 1));
}

TEST(ValuesInRange, Decreasing) {
  ImmutablePackedIntVector v({101, 100, 80, 50, 50, 2, 1, 0});
  SortedSubsequence<> ss({0, 8}, false);

  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 0, 101), Range<>(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 1, 101), Range<>(0, 7));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 2, 101), Range<>(0, 6));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 3, 101), Range<>(0, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 50, 101), Range<>(0, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 51, 101), Range<>(0, 3));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 80, 101), Range<>(0, 3));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 81, 101), Range<>(0, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 99, 101), Range<>(0, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 100, 101), Range<>(0, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 101, 101), Range<>(0, 1));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 1, 100), Range<>(1, 6));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 2, 100), Range<>(1, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 49, 100), Range<>(1, 4));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 51, 100), Range<>(1, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 50, 50), Range<>(3, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -1, 150), Range<>(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 0, 150), Range<>(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -10, 101), Range<>(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -10, -9), Range<>(8, 0));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 120, 150), Range<>(0, 0));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 100, 100), Range<>(1, 1));
}

template <typename Index>
std::vector<Range<>> GenerateRanges(Index& index, int64_t from, int64_t to) {
  std::vector<Range<>> ranges;
  index.ConsumeRanges(from, to, [&ranges](const Range<>& range) {
    ranges.emplace_back(range);
    return true;
  });

  return ranges;
}

TEST(Index, ValuesSimpleIncrement) {
  ImmutablePackedIntVector v({0, 1, 2, 50, 50, 80, 100, 101});
  SortedIntervalIndex<ImmutablePackedIntVector> index(&v);

  ASSERT_EQ(RangeSet<>(GenerateRanges(index, 0, 101)),
            RangeSet<>(std::vector<Range<>>({{0, 8}})));
  ASSERT_EQ(RangeSet<>(GenerateRanges(index, 1, 50)),
            RangeSet<>(std::vector<Range<>>({{1, 4}})));
  ASSERT_EQ(RangeSet<>(GenerateRanges(index, 5, 150)),
            RangeSet<>(std::vector<Range<>>({{3, 5}})));
}

TEST(Index, ValuesDoubleIncrement) {
  ImmutablePackedIntVector v(
      {0, 1, 2, 50, 50, 80, 100, 101, 0, 1, 2, 50, 50, 80, 100, 101});
  SortedIntervalIndex<ImmutablePackedIntVector> index(&v);

  ASSERT_EQ(RangeSet<>(GenerateRanges(index, 0, 101)),
            RangeSet<>(std::vector<Range<>>({{0, 16}})));
  ASSERT_EQ(RangeSet<>(GenerateRanges(index, 1, 50)), RangeSet<>({{1, 4}, {9, 4}}));
  ASSERT_EQ(RangeSet<>(GenerateRanges(index, 5, 150)),
            RangeSet<>({{3, 5}, {11, 5}}));
}

template <typename T>
static std::vector<Range<>> FindSlow(const std::vector<T>& v, T from, T to) {
  std::vector<Range<>> ranges;
  for (size_t i = 0; i < v.size(); ++i) {
    T el = v[i];
    if (el >= from && el <= to) {
      ranges.emplace_back(i, 1);
    }
  }

  return ranges;
}

TEST(Index, Random) {
  std::mt19937 rnd(1);
  std::uniform_int_distribution<int64_t> dist(-10000, 10000);

  std::vector<int64_t> values;
  for (size_t i = 0; i < 10000000; ++i) {
    values.emplace_back(dist(rnd));
  }

  int64_t max = *std::max_element(values.begin(), values.end());
  int64_t min = *std::min_element(values.begin(), values.end());
  ImmutablePackedIntVector v(values);
  SortedIntervalIndex<ImmutablePackedIntVector> interval_index(&v);
  BasicIndex<int64_t> basic_index(v);
  LOG(INFO) << "Index sizes " << interval_index.ByteEstimate() << " "
            << basic_index.ByteEstimate();

  std::uniform_int_distribution<int64_t> range_dist(min, max);
  for (size_t i = 0; i < 100; ++i) {
    int64_t from = dist(rnd);
    int64_t to = dist(rnd);
    if (to < from) {
      std::swap(from, to);
    }

    Timer t;
    std::vector<Range<>> to_compare_basic = GenerateRanges(basic_index, from, to);
    LOG(INFO) << "B " << t.TimeSoFarMillis().count() << " "
              << to_compare_basic.size();

    t.Reset();
    std::vector<Range<>> to_compare_interval =
        GenerateRanges(interval_index, from, to);
    LOG(INFO) << "I " << t.TimeSoFarMillis().count();

    t.Reset();
    std::vector<Range<>> to_compare_baseline = FindSlow(values, from, to);
    LOG(INFO) << "BL " << t.TimeSoFarMillis().count();

    ASSERT_EQ(RangeSet<>(to_compare_baseline), RangeSet<>(to_compare_interval));
    ASSERT_EQ(RangeSet<>(to_compare_baseline), RangeSet<>(to_compare_basic));
  }
}

TEST(IntegerStorage, Random) {
  std::mt19937 rnd(1);
  std::uniform_int_distribution<int64_t> dist(-10000, 10000);

  std::vector<int64_t> values;
  for (size_t i = 0; i < 10000000; ++i) {
    values.emplace_back(dist(rnd));
  }
  int64_t max = *std::max_element(values.begin(), values.end());
  int64_t min = *std::min_element(values.begin(), values.end());

  IntegerStorage storage;
  for (int64_t value : values) {
    storage.Add(value);
  }
  LOG(INFO) << "Done adding";
  std::cout << storage.ToString();

  std::vector<std::thread> workers;
  for (size_t thread_i = 0; thread_i < 1; ++thread_i) {
    workers.emplace_back([thread_i, min, max, &dist, &storage, &values] {
      std::mt19937 rnd(thread_i);
      std::uniform_int_distribution<int64_t> range_dist(min, max);
      for (size_t i = 0; i < 100; ++i) {
        int64_t from = dist(rnd);
        int64_t to = dist(rnd);
        if (to < from) {
          std::swap(from, to);
        }

        Timer t;
        std::vector<Range<>> to_compare = GenerateRanges(storage, from, to);
        LOG(INFO) << "I " << thread_i << " " << t.TimeSoFarMillis().count()
                  << " " << i;

        // std::vector<Range> to_compare_baseline = FindSlow(values, from, to);
        // ASSERT_EQ(RangeSet(to_compare_baseline), RangeSet(to_compare))
        //     << RangeSet(to_compare_baseline).ToString() << " vs "
        //     << RangeSet(to_compare).ToString();
      }
    });
  }

  for (size_t i = 0; i < 1; ++i) {
    workers[i].join();
  }

  std::cout << storage.ToString();
}

TEST(BoolStorage, Random) {
  std::mt19937 rnd(1);
  std::uniform_int_distribution<int64_t> dist(0, 1);

  std::vector<bool> values;
  for (size_t i = 0; i < 10000000; ++i) {
    values.push_back(i);
  }

  BoolStorage storage;
  for (bool value : values) {
    storage.Add(value);
  }
  LOG(INFO) << "Done adding";
  std::cout << storage.ToString();

  std::vector<std::thread> workers;
  for (size_t thread_i = 0; thread_i < 1; ++thread_i) {
    workers.emplace_back([thread_i, &dist, &storage, &values] {
      std::mt19937 rnd(thread_i);
      std::uniform_int_distribution<int64_t> range_dist(0, 1);
      for (size_t i = 0; i < 10000; ++i) {
        int64_t from = dist(rnd);
        int64_t to = dist(rnd);
        if (to < from) {
          std::swap(from, to);
        }

        Timer t;
        std::vector<Range<>> to_compare = GenerateRanges(storage, from, to);
        LOG(INFO) << "I " << thread_i << " " << t.TimeSoFarMillis().count()
                  << " " << i;

        // std::vector<Range> to_compare_baseline =
        //     FindSlow<bool>(values, from, to);
        // ASSERT_EQ(RangeSet(to_compare_baseline), RangeSet(to_compare))
        //     << RangeSet(to_compare_baseline).ToString() << " vs "
        //     << RangeSet(to_compare).ToString();
      }
    });
  }

  for (size_t i = 0; i < 1; ++i) {
    workers[i].join();
  }

  std::cout << storage.ToString();
}

}  // namespace
}  // namespace nc
