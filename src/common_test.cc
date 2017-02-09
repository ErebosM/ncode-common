#include <chrono>
#include <thread>

#include "common.h"
#include "map_util.h"
#include "gtest/gtest.h"

namespace nc {
namespace {

static constexpr size_t kMillion = 1000000;

TEST(Percentiles, BadValues) {
  std::vector<size_t> percentiles = Percentiles<size_t>(nullptr);
  ASSERT_TRUE(percentiles.empty());
}

TEST(Percentiles, NoValues) {
  std::vector<size_t> values;
  std::vector<size_t> percentiles = Percentiles(&values);
  ASSERT_TRUE(percentiles.empty());
}

TEST(Percentiles, SingleValue) {
  std::vector<size_t> values = {1};
  std::vector<size_t> percentiles = Percentiles(&values);
  ASSERT_EQ(101ul, percentiles.size());
  for (size_t i = 0; i < 101; ++i) {
    ASSERT_EQ(1ul, percentiles[i]);
  }
}

TEST(Percentiles, RandomValue) {
  std::vector<double> values;
  for (size_t i = 0; i < kMillion; ++i) {
    values.emplace_back(std::rand() / static_cast<double>(RAND_MAX));
  }

  std::sort(values.begin(), values.end());
  double min = values.front();
  double max = values.back();
  double med = values[kMillion / 2];

  std::vector<double> percentiles = Percentiles(&values);
  ASSERT_EQ(101ul, percentiles.size());
  ASSERT_EQ(min, percentiles.front());
  ASSERT_EQ(max, percentiles.back());
  ASSERT_EQ(med, percentiles[50]);
}

TEST(Percentiles, RandomValueTenPercentiles) {
  std::vector<double> values;
  for (size_t i = 0; i < kMillion; ++i) {
    values.emplace_back(std::rand() / static_cast<double>(RAND_MAX));
  }

  std::vector<double> percentiles = Percentiles(&values, 10);
  ASSERT_EQ(11ul, percentiles.size());
}

TEST(Bin, BadArgument) {
  std::vector<std::pair<double, double>> values = {};
  ASSERT_DEATH(Bin(0, &values), ".*");
}

TEST(Bin, Empty) {
  std::vector<std::pair<double, double>> values = {};
  Bin(10, &values);
  ASSERT_TRUE(values.empty());
}

TEST(Bin, ShortList) {
  std::vector<std::pair<double, double>> values = {
      {1.0, 1.0}, {2.0, 20.0}, {4.0, 10.0}};
  std::vector<std::pair<double, double>> binned_values = {{1.0, 31.0 / 3}};
  Bin(10, &values);

  // Bin size is 10, but there are only 3 values.
  ASSERT_EQ(binned_values, values);
}

TEST(Bin, SingleBin) {
  std::vector<std::pair<double, double>> values = {
      {1.0, 1.0}, {2.0, 20.0}, {4.0, 10.0}, {18.0, 16.0}, {18.5, 8.0}};
  std::vector<std::pair<double, double>> binned_values = {{1.0, 31.0 / 3},
                                                          {18.0, 24.0 / 2}};
  Bin(3, &values);

  // One bin, the remaining values are binned in a bin of their own.
  ASSERT_EQ(binned_values, values);
}

TEST(Bin, MultiBin) {
  std::vector<std::pair<double, double>> values = {
      {1.0, 1.0}, {2.0, 20.0}, {4.0, 10.0}, {18.0, 16.0}, {18.5, 8.0}};
  std::vector<std::pair<double, double>> binned_values = {
      {1.0, 21.0 / 2}, {4.0, 26.0 / 2}, {18.5, 8.0}};
  Bin(2, &values);

  // Two bins.
  ASSERT_EQ(binned_values, values);
}

TEST(Distribution, RandomValueTenPercentiles) {
  std::vector<double> values;
  for (size_t i = 0; i < kMillion; ++i) {
    values.emplace_back(std::rand() / static_cast<double>(RAND_MAX));
  }

  Distribution<double> distribution(&values, 10);
  std::vector<double> percentiles = Percentiles(&values, 10);
  ASSERT_EQ(percentiles, distribution.quantiles());
}

TEST(SummaryStats, NoElements) {
  SummaryStats summary_stats;
  ASSERT_EQ(0ul, summary_stats.count());
  ASSERT_DEATH(summary_stats.mean(), ".*");
  ASSERT_DEATH(summary_stats.std(), ".*");
  ASSERT_DEATH(summary_stats.var(), ".*");
}

TEST(SummaryStats, SingleElement) {
  SummaryStats summary_stats;
  summary_stats.Add(1.0);

  ASSERT_EQ(1ul, summary_stats.count());
  ASSERT_EQ(1ul, summary_stats.mean());
  ASSERT_EQ(0ul, summary_stats.std());
  ASSERT_EQ(0ul, summary_stats.var());
}

TEST(SummaryStats, Overflow) {
  double very_large_number = std::pow(std::numeric_limits<double>::max(), 0.5);
  SummaryStats summary_stats;
  ASSERT_DEATH(summary_stats.Add(very_large_number), ".*");
}

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

TEST(EmpiricalFunction, NoValues) {
  ASSERT_DEATH(Empirical2DFunction tmp_one({}, Empirical2DFunction::NEARERST),
               ".*");
  ASSERT_DEATH(
      Empirical2DFunction tmp_two({}, {}, Empirical2DFunction::NEARERST), ".*");
}

TEST(EmpiricalFunction, BadValueCount) {
  ASSERT_DEATH(Empirical2DFunction tmp_one({10.0}, {11.0, 12.0},
                                           Empirical2DFunction::NEARERST),
               ".*");
  ASSERT_DEATH(Empirical2DFunction tmp_two({10.0, 12.0}, {11.0},
                                           Empirical2DFunction::NEARERST),
               ".*");
}

TEST(EmpiricalFunction, Extrapolate) {
  Empirical2DFunction f({10.0}, {11.0}, Empirical2DFunction::NEARERST);
  ASSERT_EQ(11.0, f.Eval(10.0));
  ASSERT_EQ(11.0, f.Eval(9.99));
  f.SetLowFillValue(3.0);
  ASSERT_EQ(3.0, f.Eval(9.99));

  ASSERT_EQ(11.0, f.Eval(10.01));
  f.SetHighFillValue(300.0);
  ASSERT_EQ(300.0, f.Eval(10.01));

  Empirical2DFunction f2({10.0}, {11.0}, Empirical2DFunction::LINEAR);
  ASSERT_EQ(11.0, f2.Eval(10.01));
  ASSERT_EQ(11.0, f2.Eval(10));
  ASSERT_EQ(11.0, f2.Eval(9.99));
}

TEST(EmpiricalFunction, NearestMultiValues) {
  Empirical2DFunction f({10.0, 15.0}, {100.0, 200.0},
                        Empirical2DFunction::NEARERST);
  ASSERT_EQ(100.0, f.Eval(10.0));
  ASSERT_EQ(200.0, f.Eval(15.0));
  ASSERT_EQ(100.0, f.Eval(11.0));
  ASSERT_EQ(100.0, f.Eval(12.0));
  ASSERT_EQ(100.0, f.Eval(12.4999));
  ASSERT_EQ(200.0, f.Eval(12.5001));
  ASSERT_EQ(200.0, f.Eval(13.0));
  ASSERT_EQ(200.0, f.Eval(14.0));
}

TEST(EmpiricalFunction, LinearMultiValues) {
  Empirical2DFunction f({10.0, 15.0, -4.0}, {100.0, 200.0, -500.0},
                        Empirical2DFunction::LINEAR);
  for (double x = 10.0; x < 15.0; x += 0.01) {
    double model = 100 + (x - 10.0) / (15.0 - 10.0) * (200.0 - 100.0);
    ASSERT_NEAR(model, f.Eval(x), 0.00000001);
  }

  for (double x = -4.0; x < 10.0; x += 0.01) {
    double model = -500.0 + (x + 4.0) / (4.0 + 10.0) * (100.0 + 500.0);
    ASSERT_NEAR(model, f.Eval(x), 0.00000001);
  }
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
