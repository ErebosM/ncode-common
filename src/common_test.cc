#include <chrono>
#include <thread>

#include "common.h"
#include "map_util.h"
#include "gtest/gtest.h"

namespace nc {
namespace {

TEST(ThresholdEnforcer, DefaultPolicyDefaultMissingValue) {
  ThresholdEnforcerPolicy enforcer_policy;
  ThresholdEnforcer<int> threshold_enforcer(enforcer_policy);

  // The default policy should enforce no thresholds.
  ASSERT_EQ(0.0, threshold_enforcer.Get(1));

  ASSERT_TRUE(threshold_enforcer.Change(1, 0.0));
  ASSERT_EQ(0.0, threshold_enforcer.Get(1));

  ASSERT_TRUE(threshold_enforcer.Change(1, 0.0001));
  ASSERT_EQ(0.0001, threshold_enforcer.Get(1));
}

TEST(ThresholdEnforcer, BadValue) {
  ThresholdEnforcerPolicy enforcer_policy;
  enforcer_policy.set_empty_threshold_absolute(0.0);
  enforcer_policy.set_empty_threshold_absolute(1.0);
  ASSERT_DEATH(enforcer_policy.set_empty_threshold_absolute(-1.0), ".*");

  enforcer_policy.set_threshold_absolute(0.0);
  enforcer_policy.set_threshold_absolute(1.0);
  ASSERT_DEATH(enforcer_policy.set_threshold_absolute(-1.0), ".*");

  enforcer_policy.set_threshold_relative_to_total(1.0);
  enforcer_policy.set_threshold_relative_to_total(0.0);
  ASSERT_DEATH(enforcer_policy.set_threshold_relative_to_total(1.5), ".*");
  ASSERT_DEATH(enforcer_policy.set_threshold_relative_to_total(-1.5), ".*");

  enforcer_policy.set_threshold_relative_to_current(1.0);
  enforcer_policy.set_threshold_relative_to_current(0.5);
  ASSERT_DEATH(enforcer_policy.set_threshold_relative_to_current(1.5), ".*");
  ASSERT_DEATH(enforcer_policy.set_threshold_relative_to_current(-1.5), ".*");
}

TEST(ThresholdEnforcer, AbsoluteEmptyThreshold) {
  ThresholdEnforcerPolicy enforcer_policy;
  enforcer_policy.set_empty_threshold_absolute(1.0);
  ThresholdEnforcer<int> threshold_enforcer(enforcer_policy);

  ASSERT_FALSE(threshold_enforcer.Change(1, 0.0));
  ASSERT_FALSE(threshold_enforcer.Change(1, 0.5));
  ASSERT_EQ(0.0, threshold_enforcer.Get(1));

  ASSERT_FALSE(threshold_enforcer.Change(2, 0.99));
  ASSERT_EQ(0.0, threshold_enforcer.Get(2));

  ASSERT_TRUE(threshold_enforcer.Change(2, -1.0));
  ASSERT_EQ(-1.0, threshold_enforcer.Get(2));

  ASSERT_TRUE(threshold_enforcer.Change(2, -1.1));
  ASSERT_EQ(-1.1, threshold_enforcer.Get(2));

  ASSERT_TRUE(threshold_enforcer.Change(2, 1.0));
  ASSERT_EQ(1.0, threshold_enforcer.Get(2));

  ASSERT_TRUE(threshold_enforcer.Change(2, 1.1));
  ASSERT_EQ(1.1, threshold_enforcer.Get(2));
}

TEST(ThresholdEnforcer, AbsoluteThreshold) {
  ThresholdEnforcerPolicy enforcer_policy;
  enforcer_policy.set_threshold_absolute(1.0);
  enforcer_policy.set_empty_threshold_absolute(10.0);
  ThresholdEnforcer<int> threshold_enforcer(enforcer_policy);

  ASSERT_FALSE(threshold_enforcer.Change(1, 0.0));
  ASSERT_EQ(0.0, threshold_enforcer.Get(1));

  ASSERT_FALSE(threshold_enforcer.Change(1, 0.5));
  ASSERT_EQ(0.0, threshold_enforcer.Get(1));

  ASSERT_FALSE(threshold_enforcer.Change(1, 0.99));
  ASSERT_EQ(0.0, threshold_enforcer.Get(1));

  ASSERT_FALSE(threshold_enforcer.Change(5, 0.99));
  ASSERT_EQ(0.0, threshold_enforcer.Get(1));

  ASSERT_TRUE(threshold_enforcer.Change(2, 10.0));
  ASSERT_EQ(10.0, threshold_enforcer.Get(2));

  ASSERT_FALSE(threshold_enforcer.Change(2, 10.5));
  ASSERT_EQ(10.0, threshold_enforcer.Get(2));

  ASSERT_FALSE(threshold_enforcer.Change(2, 9.5));
  ASSERT_EQ(10.0, threshold_enforcer.Get(2));

  ASSERT_TRUE(threshold_enforcer.Change(2, 11.5));
  ASSERT_EQ(11.5, threshold_enforcer.Get(2));
}

TEST(ThresholdEnforcer, RelativeFromTotal) {
  ThresholdEnforcerPolicy enforcer_policy;
  enforcer_policy.set_threshold_relative_to_total(0.1);
  ThresholdEnforcer<int> threshold_enforcer(enforcer_policy);

  ASSERT_TRUE(threshold_enforcer.Change(1, 5.0));
  ASSERT_TRUE(threshold_enforcer.Change(2, 5.0));
  ASSERT_TRUE(threshold_enforcer.Change(3, 20.0));

  // Too low -- 10% of 30 is 3.
  ASSERT_FALSE(threshold_enforcer.Change(4, 2.0));

  ASSERT_TRUE(threshold_enforcer.Change(3, 3.0));

  // Should be fine now.
  ASSERT_TRUE(threshold_enforcer.Change(4, 2.0));
}

TEST(ThresholdEnforcer, BulkChange) {
  ThresholdEnforcerPolicy enforcer_policy;
  enforcer_policy.set_threshold_absolute(1.0);
  ThresholdEnforcer<int> threshold_enforcer(enforcer_policy);

  ASSERT_FALSE(
      threshold_enforcer.ChangeBulk({{1, 0.1}, {2, 0.2}, {3, 0.9}, {4, -0.5}}));
  ASSERT_TRUE(
      threshold_enforcer.ChangeBulk({{1, 0.1}, {2, 0.2}, {3, 1.0}, {4, -0.5}}));
  ASSERT_EQ(0.1, threshold_enforcer.Get(1));

  ASSERT_FALSE(threshold_enforcer.ChangeBulk({{1, 0.1}, {3, 1.0}, {4, -0.5}}));
  ASSERT_TRUE(threshold_enforcer.ChangeBulk({{1, 0.1}, {4, -0.5}}));
  ASSERT_EQ(0.0, threshold_enforcer.Get(2));
  ASSERT_EQ(0.0, threshold_enforcer.Get(3));
}

TEST(TimeoutEnforcer, DefaultPolicy) {
  TimeoutPolicy timeout_policy;
  TimeoutEnforcer<int> timeout_enforcer(timeout_policy);
  timeout_enforcer.Update(1, 10);
  timeout_enforcer.Update(2, 20);

  // Time has advanced past 10.
  ASSERT_DEATH(timeout_enforcer.Timeout(10), ".*");

  // By default the enforcer will immediately time values out.
  ASSERT_EQ(std::vector<int>({1, 2}), timeout_enforcer.Timeout(20));
}

TEST(TimeoutEnforcer, BadUpdate) {
  TimeoutPolicy timeout_policy;
  TimeoutEnforcer<int> timeout_enforcer(timeout_policy);
  timeout_enforcer.Update(1, 10);
  timeout_enforcer.Update(1, 10);
  ASSERT_DEATH(timeout_enforcer.Update(1, 9), ".*");
}

TEST(TimeoutEnforcer, SingleKey) {
  TimeoutPolicy timeout_policy;
  timeout_policy.set_base_timeout(100);

  TimeoutEnforcer<int> timeout_enforcer(timeout_policy);
  timeout_enforcer.Update(1, 10);
  ASSERT_EQ(std::vector<int>({}), timeout_enforcer.Timeout(10));
  ASSERT_EQ(std::vector<int>({}), timeout_enforcer.Timeout(100));
  ASSERT_EQ(std::vector<int>({1}), timeout_enforcer.Timeout(110));
  ASSERT_EQ(std::vector<int>({}), timeout_enforcer.Timeout(500));

  timeout_enforcer.Update(2, 10);
  timeout_enforcer.Update(2, 50);
  timeout_enforcer.Update(2, 100);
  ASSERT_EQ(std::vector<int>({}), timeout_enforcer.Timeout(190));
  ASSERT_EQ(std::vector<int>({2}), timeout_enforcer.Timeout(250));
}

TEST(TimeoutEnforcer, SingleKeyMultiRemove) {
  TimeoutPolicy timeout_policy;
  timeout_policy.set_base_timeout(100);

  TimeoutEnforcer<int> timeout_enforcer(timeout_policy);
  timeout_enforcer.Update(1, 10);
  ASSERT_EQ(std::vector<int>({1}), timeout_enforcer.Timeout(200));
  timeout_enforcer.Update(1, 210);
  ASSERT_EQ(std::vector<int>({1}), timeout_enforcer.Timeout(400));
}

TEST(TimeoutEnforcer, SingleKeyPenalty) {
  TimeoutPolicy timeout_policy;
  timeout_policy.set_base_timeout(100);
  timeout_policy.set_timeout_penalty(100);
  timeout_policy.set_timeout_penalty_lookback(500);

  TimeoutEnforcer<int> timeout_enforcer(timeout_policy);
  timeout_enforcer.Update(1, 10);
  ASSERT_EQ(std::vector<int>({1}), timeout_enforcer.Timeout(150));

  // The update for the key (200) is within 500 of the last update (10), so the
  // penalty should kick in.
  timeout_enforcer.Update(1, 200);
  ASSERT_EQ(std::vector<int>({}), timeout_enforcer.Timeout(350));

  // 400 is 100 (base) + 100 (penalty) from the last update (200).
  ASSERT_EQ(std::vector<int>({1}), timeout_enforcer.Timeout(400));
}

TEST(TimeoutEnforcer, SingleKeyPenaltyCumulative) {
  TimeoutPolicy timeout_policy;
  timeout_policy.set_base_timeout(100);
  timeout_policy.set_timeout_penalty(100);
  timeout_policy.set_timeout_penalty_lookback(500);
  timeout_policy.set_timeout_penalty_cumulative(true);

  TimeoutEnforcer<int> timeout_enforcer(timeout_policy);
  timeout_enforcer.Update(1, 10);
  ASSERT_EQ(std::vector<int>({1}), timeout_enforcer.Timeout(150));

  // The update for the key (200) is within 500 of the last update (10), so the
  // penalty should kick in.
  timeout_enforcer.Update(1, 200);
  ASSERT_EQ(std::vector<int>({}), timeout_enforcer.Timeout(350));

  // Another update within the penalty lookback.
  timeout_enforcer.Update(1, 400);

  // 100 away from last update, but 2x penalty.
  ASSERT_EQ(std::vector<int>({}), timeout_enforcer.Timeout(500));

  // 700 is 100 (base) + 200 (penalty) from the last update (400).
  ASSERT_EQ(std::vector<int>({1}), timeout_enforcer.Timeout(700));
}

TEST(CountdownTimer, Timer) {
  CountdownTimer timer(std::chrono::milliseconds(100));
  ASSERT_FALSE(timer.Expired());
  ASSERT_TRUE(timer.RemainingTime() > std::chrono::nanoseconds::zero());
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  ASSERT_TRUE(timer.Expired());
  ASSERT_TRUE(timer.RemainingTime() == std::chrono::nanoseconds::zero());
  ASSERT_TRUE(timer.Expired());
}

}  // namespace
}  // namespace nc
