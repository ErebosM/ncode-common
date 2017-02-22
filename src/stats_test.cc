#include <chrono>
#include <thread>

#include "stats.h"
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

TEST(SumConvolute, Simple) {
  std::map<int, double> prob_a = {{1, 1.0 / 6}, {2, 1.0 / 6}, {3, 1.0 / 6},
                                  {4, 1.0 / 6}, {5, 1.0 / 6}, {6, 1.0 / 6}};
  std::map<int, double> prob_b = prob_a;

  std::map<int, double> a_and_b = SumConvolute(prob_a, prob_b);
  ASSERT_NEAR(1.0/36, a_and_b[2], 0.00001);
  ASSERT_NEAR(2.0/36, a_and_b[3], 0.00001);
  ASSERT_NEAR(3.0/36, a_and_b[4], 0.00001);
  ASSERT_NEAR(4.0/36, a_and_b[5], 0.0001);
  ASSERT_NEAR(5.0/36, a_and_b[6], 0.0001);
  ASSERT_NEAR(6.0/36, a_and_b[7], 0.0001);
  ASSERT_NEAR(5.0/36, a_and_b[8], 0.0001);
  ASSERT_NEAR(4.0/36, a_and_b[9], 0.0001);
  ASSERT_NEAR(3.0/36, a_and_b[10], 0.0001);
  ASSERT_NEAR(2.0/36, a_and_b[11], 0.0001);
  ASSERT_NEAR(1.0/36, a_and_b[12], 0.0001);
}

}  // namespace
}  // namespace nc
