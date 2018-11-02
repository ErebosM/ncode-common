#include "interval_tree.h"

#include <thread>
#include "gtest/gtest.h"

namespace nc {
namespace interval_tree {

using TestInterval = Interval<double, bool>;
using TestTree = TreeRoot<double, bool>;

static std::vector<TestInterval> LookupWrapper(const TestTree& tree,
                                               double point) {
  std::vector<TestInterval> out;
  tree.Lookup(point, [&out](const TestInterval& interval, uint32_t i) {
    Unused(i);
    out.emplace_back(interval);
    return true;
  });

  return out;
}

TEST(IntervalTree, Empty) {
  std::vector<TestInterval> intervals = {};
  TestTree tree(intervals);
  ASSERT_TRUE(LookupWrapper(tree, 0.0).empty());
}

TEST(IntervalTree, SingleInterval) {
  std::vector<TestInterval> intervals = {{10.0, 100.0, true}};
  TestTree tree(intervals);
  ASSERT_TRUE(LookupWrapper(tree, 0.0).empty());
  ASSERT_TRUE(LookupWrapper(tree, -10.0).empty());
  ASSERT_EQ(intervals, LookupWrapper(tree, 10.0));
  ASSERT_EQ(intervals, LookupWrapper(tree, 100.0));
  ASSERT_EQ(intervals, LookupWrapper(tree, 50.0));
}

TEST(IntervalTree, TwoIntervals) {
  std::vector<TestInterval> intervals = {{10.0, 100.0, true},
                                         {-10.0, 0.0, true}};
  TestTree tree(intervals);
  ASSERT_TRUE(LookupWrapper(tree, 1.0).empty());
  ASSERT_TRUE(LookupWrapper(tree, -100.0).empty());
  ASSERT_TRUE(LookupWrapper(tree, 101.0).empty());

  std::vector<TestInterval> single_interval = {{10.0, 100.0, true}};
  ASSERT_EQ(single_interval, LookupWrapper(tree, 10.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 100.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 50.0));

  single_interval = {{-10.0, 0.0, true}};
  ASSERT_EQ(single_interval, LookupWrapper(tree, -10.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 0.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, -5.0));
}

TEST(IntervalTree, TwoIntervalsSame) {
  std::vector<TestInterval> intervals = {{10.0, 100.0, true},
                                         {10.0, 100.0, true}};
  TestTree tree(intervals);
  ASSERT_TRUE(LookupWrapper(tree, 0.0).empty());
  ASSERT_TRUE(LookupWrapper(tree, -10.0).empty());
  ASSERT_EQ(intervals, LookupWrapper(tree, 10.0));
  ASSERT_EQ(intervals, LookupWrapper(tree, 100.0));
  ASSERT_EQ(intervals, LookupWrapper(tree, 50.0));
}

TEST(IntervalTree, TwoIntervalsOverlappingEdges) {
  std::vector<TestInterval> intervals = {{10.0, 100.0, true},
                                         {100.0, 110.0, true}};
  TestTree tree(intervals);
  ASSERT_TRUE(LookupWrapper(tree, 111.0).empty());
  ASSERT_TRUE(LookupWrapper(tree, 9.0).empty());
  ASSERT_EQ(intervals, LookupWrapper(tree, 100.0));

  std::vector<TestInterval> single_interval = {{10.0, 100.0, true}};
  ASSERT_EQ(single_interval, LookupWrapper(tree, 10.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 15.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 99.0));

  single_interval = {{100.0, 110.0, true}};
  ASSERT_EQ(single_interval, LookupWrapper(tree, 101.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 105.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 110.0));
}

TEST(IntervalTree, TwoIntervalsOverlapping) {
  std::vector<TestInterval> intervals = {{10.0, 150.0, true},
                                         {100.0, 110.0, true}};
  TestTree tree(intervals);
  ASSERT_TRUE(LookupWrapper(tree, 9.0).empty());
  ASSERT_EQ(intervals, LookupWrapper(tree, 100.0));
  ASSERT_EQ(intervals, LookupWrapper(tree, 110.0));
  ASSERT_EQ(intervals, LookupWrapper(tree, 101.0));
  ASSERT_EQ(intervals, LookupWrapper(tree, 105.0));

  std::vector<TestInterval> single_interval = {{10.0, 150.0, true}};
  ASSERT_EQ(single_interval, LookupWrapper(tree, 10.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 15.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 99.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 140.0));
  ASSERT_EQ(single_interval, LookupWrapper(tree, 150.0));
}

}  // namespace interval_tree
}  // namespace nc
