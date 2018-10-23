#include "num_col.h"
#include "gtest/gtest.h"

#include <random>

namespace nc {
namespace {

TEST(RangeSet, Init) {
  RangeSet set(std::vector<Range>({}));
  ASSERT_TRUE(set.ranges().empty());
}

TEST(RangeSet, Single) {
  RangeSet set(std::vector<Range>({{10, 15}}));

  std::vector<Range> ranges = {{10, 15}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, SingleZeroSize) {
  RangeSet set(std::vector<Range>({{10, 0}}));

  std::vector<Range> ranges = {};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, Sorted) {
  RangeSet set({{10, 15}, {5, 2}});

  std::vector<Range> ranges = {{5, 2}, {10, 15}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, MergeTwo) {
  RangeSet set({{10, 15}, {5, 5}});

  std::vector<Range> ranges = {{5, 20}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, MergeZeroSize) {
  RangeSet set({{10, 0}, {10, 0}, {10, 0}});

  std::vector<Range> ranges = {};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, Duplicates) {
  RangeSet set({{10, 1}, {10, 1}, {10, 1}, {100, 0}});

  std::vector<Range> ranges = {{10, 1}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, MergeSingle) {
  RangeSet set({{10, 1}, {11, 1}, {12, 1}});

  std::vector<Range> ranges = {{10, 3}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(RangeSet, OverlappingMerge) {
  RangeSet set({{10, 15}, {5, 10}});

  std::vector<Range> ranges = {{5, 20}};
  ASSERT_EQ(ranges, set.ranges());
}

TEST(ImmutablePackedIntVector, Empty) {
  std::vector<int64_t> empty;
  ImmutablePackedIntVector v(empty);
  ASSERT_EQ(0ul, v.size());
}

TEST(ImmutablePackedIntVector, SingleValue) {
  ImmutablePackedIntVector v({42});
  ASSERT_EQ(1ul, v.size());
  ASSERT_EQ(42, v.ValueAt(0));
}

TEST(ImmutablePackedIntVector, SingleValueNegative) {
  ImmutablePackedIntVector v({-42});
  ASSERT_EQ(1ul, v.size());
  ASSERT_EQ(-42, v.ValueAt(0));

  auto stats = v.Stats();
  ASSERT_TRUE(stats.zig_zagged);
}

TEST(ImmutablePackedIntVector, SameValues) {
  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(1000000 + 42);
  }

  ImmutablePackedIntVector v(values);
  ASSERT_EQ(1000000ul, v.size());

  for (size_t i = 0; i < 1000000; ++i) {
    ASSERT_EQ(1000000 + 42, v.ValueAt(i));
  }

  auto stats = v.Stats();
  ASSERT_FALSE(stats.zig_zagged);
  ASSERT_NEAR(1000000UL, stats.byte_size_estimate, 100);
}

TEST(ImmutablePackedIntVector, Values) {
  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(i);
  }

  ImmutablePackedIntVector v(values);
  ASSERT_EQ(1000000ul, v.size());

  for (int i = 0; i < 1000000; ++i) {
    ASSERT_EQ(i, v.ValueAt(i));
  }

  auto stats = v.Stats();
  ASSERT_FALSE(stats.zig_zagged);
  ASSERT_NEAR(3000000UL, stats.byte_size_estimate, 100);
}

TEST(ImmutablePackedIntVector, ValuesReverse) {
  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(1000000 - i);
  }

  ImmutablePackedIntVector v(values);
  ASSERT_EQ(1000000ul, v.size());

  for (int i = 0; i < 1000000; ++i) {
    ASSERT_EQ(1000000 - i, v.ValueAt(i));
  }

  auto stats = v.Stats();
  ASSERT_FALSE(stats.zig_zagged);
  ASSERT_NEAR(3000000UL, stats.byte_size_estimate, 100);
}

TEST(ImmutablePackedIntVector, RandomValues) {
  std::mt19937 rnd(1);
  std::uniform_int_distribution<int64_t> dist;

  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(dist(rnd));
  }

  ImmutablePackedIntVector v(values);
  ASSERT_EQ(1000000ul, v.size());

  for (size_t i = 0; i < 1000000; ++i) {
    ASSERT_EQ(values[i], v.ValueAt(i));
  }

  auto stats = v.Stats();
  ASSERT_NEAR(8000000UL, stats.byte_size_estimate, 100);
}

TEST(SortedSubsequence, Empty) {
  std::vector<int64_t> values;
  ImmutablePackedIntVector v(values);

  ASSERT_TRUE(SortedSubsequence<int64_t>::Get(&v).empty());
}

TEST(SortedSubsequence, SingleValue) {
  ImmutablePackedIntVector v({10});

  std::vector<SortedSubsequence<int64_t>> ss_model = {{{0, 1}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<int64_t>::Get(&v));
}

TEST(SortedSubsequence, Increasing) {
  ImmutablePackedIntVector v({10, 11, 12, 13, 14, 15, 16});

  std::vector<SortedSubsequence<int64_t>> ss_model = {{{0, 7}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<int64_t>::Get(&v));
}

TEST(SortedSubsequence, Decreasing) {
  ImmutablePackedIntVector v({16, 15, 14, 13, 12, 11, 10});

  std::vector<SortedSubsequence<int64_t>> ss_model = {{{0, 7}, false}};
  ASSERT_EQ(ss_model, SortedSubsequence<int64_t>::Get(&v));
}

TEST(SortedSubsequence, IncreasingIncreasing) {
  ImmutablePackedIntVector v({10, 11, 5});

  std::vector<SortedSubsequence<int64_t>> ss_model = {{{0, 2}, true},
                                                      {{2, 1}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<int64_t>::Get(&v));
}

TEST(SortedSubsequence, IncreasingIncreasingTwo) {
  ImmutablePackedIntVector v({10, 11, 5, 50});

  std::vector<SortedSubsequence<int64_t>> ss_model = {{{0, 2}, true},
                                                      {{2, 2}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<int64_t>::Get(&v));
}

TEST(SortedSubsequence, DecreasingIncreasingIncreasing) {
  ImmutablePackedIntVector v({10, 9, 8, 50, 60, 80, 1, 2, 3});

  std::vector<SortedSubsequence<int64_t>> ss_model = {
      {{0, 3}, false}, {{3, 3}, true}, {{6, 3}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<int64_t>::Get(&v));
}

TEST(SortedSubsequence, IncreasingRepeated) {
  ImmutablePackedIntVector v({10, 10, 10, 10, 11, 5, 5});

  std::vector<SortedSubsequence<int64_t>> ss_model = {{{0, 5}, true},
                                                      {{5, 2}, true}};
  ASSERT_EQ(ss_model, SortedSubsequence<int64_t>::Get(&v));
}

TEST(SortedSubsequence, LotsOfValues) {
  std::mt19937 rnd(1);
  std::uniform_int_distribution<int64_t> dist;

  std::vector<int64_t> values;
  for (size_t i = 0; i < 1000000; ++i) {
    values.emplace_back(dist(rnd));
  }

  ImmutablePackedIntVector v(values);

  const std::vector<SortedSubsequence<int64_t>>& ss =
      SortedSubsequence<int64_t>::Get(&v);
  LOG(INFO) << ss.size();
  LOG(INFO) << sizeof(SortedSubsequence<int64_t>) << " " << sizeof(Range);

  // std::vector<SortedSubsequence> ss_model = {
  //     {{2, 1}, false}, {{0, 2}, true}, {{3, 1}, true}};
  // ASSERT_EQ(ss_model, ss);
}

TEST(SortedSubsequence, BadRange) {
  ImmutablePackedIntVector v({0, 1});
  ASSERT_DEATH(SortedSubsequence<int64_t> ss({0, 0}, true), ".*");
}

TEST(Bound, Increasing) {
  ImmutablePackedIntVector v({0, 1, 2, 50, 50, 80, 100, 101});
  SortedSubsequence<int64_t> ss({0, 8}, true);

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
  SortedSubsequence<int64_t> ss({0, 8}, false);

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
  SortedSubsequence<int64_t> ss({0, 8}, true);

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
  SortedSubsequence<int64_t> ss({0, 8}, true);

  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 0, 101), Range(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 1, 101), Range(1, 7));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 2, 101), Range(2, 6));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 3, 101), Range(3, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 50, 101), Range(3, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 51, 101), Range(5, 3));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 80, 101), Range(5, 3));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 81, 101), Range(6, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 99, 101), Range(6, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 100, 101), Range(6, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 101, 101), Range(7, 1));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 1, 100), Range(1, 6));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 2, 100), Range(2, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 49, 100), Range(3, 4));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 51, 100), Range(5, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 50, 50), Range(3, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -1, 150), Range(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 0, 150), Range(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -10, 101), Range(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -10, -9), Range(0, 0));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 120, 150), Range(8, 0));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 100, 100), Range(6, 1));
}

TEST(ValuesInRange, Decreasing) {
  ImmutablePackedIntVector v({101, 100, 80, 50, 50, 2, 1, 0});
  SortedSubsequence<int64_t> ss({0, 8}, false);

  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 0, 101), Range(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 1, 101), Range(0, 7));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 2, 101), Range(0, 6));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 3, 101), Range(0, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 50, 101), Range(0, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 51, 101), Range(0, 3));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 80, 101), Range(0, 3));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 81, 101), Range(0, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 99, 101), Range(0, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 100, 101), Range(0, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 101, 101), Range(0, 1));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 1, 100), Range(1, 6));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 2, 100), Range(1, 5));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 49, 100), Range(1, 4));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 51, 100), Range(1, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 50, 50), Range(3, 2));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -1, 150), Range(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 0, 150), Range(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -10, 101), Range(0, 8));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, -10, -9), Range(8, 0));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 120, 150), Range(0, 0));
  ASSERT_EQ(ss.IndicesOfValuesInRange(v, 100, 100), Range(1, 1));
}

TEST(Index, ValuesSimpleIncrement) {
  ImmutablePackedIntVector v({0, 1, 2, 50, 50, 80, 100, 101});
  SortedIntervalIndex<int64_t> index(&v);

  ASSERT_EQ(index.IndicesOfValuesInRange(0, 101),
            RangeSet(std::vector<Range>({{0, 8}})));
  ASSERT_EQ(index.IndicesOfValuesInRange(1, 50),
            RangeSet(std::vector<Range>({{1, 4}})));
  ASSERT_EQ(index.IndicesOfValuesInRange(5, 150),
            RangeSet(std::vector<Range>({{3, 5}})));
}

TEST(Index, ValuesDoubleIncrement) {
  ImmutablePackedIntVector v(
      {0, 1, 2, 50, 50, 80, 100, 101, 0, 1, 2, 50, 50, 80, 100, 101});
  SortedIntervalIndex<int64_t> index(&v);

  ASSERT_EQ(index.IndicesOfValuesInRange(0, 101),
            RangeSet(std::vector<Range>({{0, 16}})));
  ASSERT_EQ(index.IndicesOfValuesInRange(1, 50), RangeSet({{1, 4}, {9, 4}}));
  ASSERT_EQ(index.IndicesOfValuesInRange(5, 150), RangeSet({{3, 5}, {11, 5}}));
}

static RangeSet FindSlow(const std::vector<int64_t>& v, int64_t from,
                         int64_t to) {
  std::vector<Range> ranges;
  for (size_t i = 0; i < v.size(); ++i) {
    int64_t el = v[i];
    if (el >= from && el <= to) {
      ranges.emplace_back(i, 1);
    }
  }

  return RangeSet(&ranges);
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
  SortedIntervalIndex<int64_t> interval_index(&v);
  BasicIndex<int64_t> basic_index(v);
  LOG(INFO) << "Index sizes " << interval_index.ByteEstimate() << " "
            << basic_index.ByteEstimate();

  std::uniform_int_distribution<int64_t> range_dist(min, max);
  for (size_t i = 0; i < 10; ++i) {
    int64_t from = dist(rnd);
    int64_t to = dist(rnd);
    if (to < from) {
      std::swap(from, to);
    }

    Timer t;
    RangeSet to_compare_basic = basic_index.IndicesOfValuesInRange(from, to);
    LOG(INFO) << "B " << t.TimeSoFarMillis().count();

    t.Reset();
    RangeSet to_compare_interval =
        interval_index.IndicesOfValuesInRange(from, to);
    LOG(INFO) << "I " << t.TimeSoFarMillis().count();

    RangeSet model = FindSlow(values, from, to);
    ASSERT_EQ(model, RangeSet(to_compare_interval));
    ASSERT_EQ(model, RangeSet(to_compare_basic));
  }
}

}  // namespace
}  // namespace nc
