#include "grapher.h"

#include <initializer_list>
#include <tuple>

#include "gtest/gtest.h"
#include "web_page.h"
#include "../file.h"

namespace nc {
namespace viz {
namespace {

struct DummyKey {
  constexpr explicit DummyKey(int key) : key(key) {}
  constexpr DummyKey() : key(0) {}

  friend bool operator==(const DummyKey& a, const DummyKey& b) {
    return a.key == b.key;
  }

  int key;
};

class TestSequence : public PeriodicSequenceIntefrace {
 public:
  TestSequence(const std::vector<std::pair<size_t, double>>& periods_and_values)
      : periods_and_values_(periods_and_values) {}

  size_t size() const override { return periods_and_values_.size(); }
  void at(size_t i, size_t* period_index, double* value) const override {
    std::tie(*period_index, *value) = periods_and_values_[i];
  }

 private:
  std::vector<std::pair<size_t, double>> periods_and_values_;
};

using TestRanker = Ranker<DummyKey>;
using ReturnVector = std::vector<std::pair<DummyKey, std::vector<double>>>;

static constexpr DummyKey kDefaultDummyKey = DummyKey();

void CheckForKey(const ReturnVector& return_vector, const DummyKey& key,
                 std::vector<double> values) {
  bool found_key = false;
  for (const std::pair<DummyKey, std::vector<double>>& key_and_values :
       return_vector) {
    const DummyKey& key_in_return_vector = key_and_values.first;
    const std::vector<double>& values_in_return_vector = key_and_values.second;

    if (key.key == key_in_return_vector.key) {
      ASSERT_FALSE(found_key);
      found_key = true;
      ASSERT_EQ(values, values_in_return_vector);
    }
  }
  ASSERT_TRUE(found_key);
}

TEST(PerPeriodClassifier, Empty) {
  TestRanker ranker(10);
  ASSERT_TRUE(ranker.GetTopN(kDefaultDummyKey).empty());
}

TEST(PerPeriodClassifier, SingleKeySingleDatum) {
  TestRanker ranker_0(0);
  TestRanker ranker_1(1);
  TestRanker ranker_10(10);
  ranker_0.AddData(DummyKey(1), TestSequence({{0, 10}}));
  ranker_1.AddData(DummyKey(1), TestSequence({{0, 10}}));
  ranker_10.AddData(DummyKey(1), TestSequence({{0, 10}}));

  ReturnVector out = ranker_0.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(1ul, out.size());
  CheckForKey(out, kDefaultDummyKey, {10});

  out = ranker_1.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(1ul, out.size());
  CheckForKey(out, DummyKey(1), {10});

  ReturnVector out_two = ranker_10.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(out, out_two);
}

TEST(PerPeriodClassifier, SingleKeySingleDatumGap) {
  TestRanker ranker(1);
  ranker.AddData(DummyKey(1), TestSequence({{9, 10}}));

  ReturnVector out = ranker.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(1ul, out.size());
  CheckForKey(out, DummyKey(1), {0, 0, 0, 0, 0, 0, 0, 0, 0, 10});
}

TEST(PerPeriodClassifier, SingleKeySamePeriod) {
  TestRanker ranker(1);
  ranker.AddData(DummyKey(1), TestSequence({{0, 10}, {0, 20}}));

  ReturnVector out = ranker.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(1ul, out.size());
  CheckForKey(out, DummyKey(1), {20 + 10});
}

TEST(PerPeriodClassifier, MultiKey) {
  TestRanker ranker_0(0);
  TestRanker ranker_1(1);
  TestRanker ranker_10(10);
  ranker_0.AddData(DummyKey(1), TestSequence({{0, 10}}));
  ranker_0.AddData(DummyKey(2), TestSequence({{0, 20}}));
  ranker_1.AddData(DummyKey(1), TestSequence({{0, 10}}));
  ranker_1.AddData(DummyKey(2), TestSequence({{0, 20}}));
  ranker_10.AddData(DummyKey(1), TestSequence({{0, 10}}));
  ranker_10.AddData(DummyKey(2), TestSequence({{0, 20}}));

  ReturnVector out = ranker_0.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(1ul, out.size());
  CheckForKey(out, kDefaultDummyKey, {20 + 10});

  out = ranker_1.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(2ul, out.size());
  CheckForKey(out, kDefaultDummyKey, {10});
  CheckForKey(out, DummyKey(2), {20});

  out = ranker_10.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(2ul, out.size());
  CheckForKey(out, DummyKey(1), {10});
  CheckForKey(out, DummyKey(2), {20});
}

TEST(PerPeriodClassifier, MultiKeyLocalRank0) {
  TestRanker r1(0, 20, 50);
  TestRanker r2(0, 12, 50);
  TestRanker r3(0, 12, 12);
  TestRanker r4(0, 60, 61);

  r1.AddData(DummyKey(1), TestSequence({{10, 10}}));
  r1.AddData(DummyKey(2), TestSequence({{12, 20}}));
  r2.AddData(DummyKey(1), TestSequence({{10, 10}}));
  r2.AddData(DummyKey(2), TestSequence({{12, 20}}));
  r3.AddData(DummyKey(1), TestSequence({{10, 10}}));
  r3.AddData(DummyKey(2), TestSequence({{12, 20}}));
  r4.AddData(DummyKey(1), TestSequence({{10, 10}}));
  r4.AddData(DummyKey(2), TestSequence({{12, 20}}));

  ReturnVector out = r1.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(0ul, out.size());

  out = r2.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(1ul, out.size());
  CheckForKey(out, kDefaultDummyKey, {20});

  // Range is exclusive of last element -- like in a for loop.
  out = r3.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(0ul, out.size());

  out = r4.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(0ul, out.size());
}

TEST(PerPeriodClassifier, MultiKeyLocalRank1) {
  TestRanker r1(1, 9, 50);
  r1.AddData(DummyKey(1), TestSequence({{10, 10}}));
  r1.AddData(DummyKey(2), TestSequence({{12, 20}}));

  ReturnVector out = r1.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(2ul, out.size());
  CheckForKey(out, kDefaultDummyKey, {0, 10, 0, 0});
  CheckForKey(out, DummyKey(2), {0, 0, 0, 20});
}

TEST(PerPeriodClassifier, MultiKeyLocalRank2) {
  TestRanker r1(2, 10, 50);
  r1.AddData(DummyKey(1), TestSequence({{10, 10}}));
  r1.AddData(DummyKey(2), TestSequence({{12, 20}}));

  ReturnVector out = r1.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(2ul, out.size());
  CheckForKey(out, DummyKey(1), {10, 0, 0});
  CheckForKey(out, DummyKey(2), {0, 0, 20});
}

TEST(PerPeriodClassifier, BadRegion) {
  ASSERT_DEATH(TestRanker ranker(0, 50, 20), ".*");
}

TEST(PerPeriodClassifier, MultiValue) {
  TestRanker ranker(2);
  ranker.AddData(DummyKey(1),
                 TestSequence({{5, 45}, {10, 10}, {11, 12}, {13, 13}}));

  ReturnVector out = ranker.GetTopN(kDefaultDummyKey);
  ASSERT_EQ(1ul, out.size());
  CheckForKey(out, DummyKey(1), {0, 0, 0, 0, 0, 45, 0, 0, 0, 0, 10, 12, 0, 13});
}

TEST(PythonOutput, CDF) {
  CDFPlot cdf_plot;
  cdf_plot.AddData<double>("", {1.0, 2.0, 4.0, 3.0, 5.0});

  HtmlPage page;
  cdf_plot.PlotToHtml(&page);
  nc::File::WriteStringToFile(page.Construct(), "cdf.html");
}

TEST(PythonOutput, StackedLine) {
  StackedLinePlot plot({1.0, 2.0, 4.0, 3.0, 5.0});
  plot.AddData(
      "series one",
      {{1.0, 10.0}, {2.0, 10.0}, {4.0, 34.0}, {3.0, 67.0}, {5.0, 32.0}});
  plot.AddData("series two",
               {{1.5, 1.0}, {2.5, 1.0}, {4.5, 10.0}, {3.5, 25.0}, {5.5, 32.0}});

  HtmlPage page;
  plot.PlotToHtml(&page);
  nc::File::WriteStringToFile(page.Construct(), "stacked.html");
}

TEST(PythonOutput, Bar) {
  BarPlot plot({"D1", "D2", "D3", "D4", "D5"});
  plot.AddData<double>("series one", {1.0, 2.0, 4.0, 3.0, 5.0});
  plot.AddData<double>("series two", {4.0, 2.0, 8.0, 1.0, 6.0});

  HtmlPage page;
  plot.PlotToHtml(&page);
  nc::File::WriteStringToFile(page.Construct(), "bar.html");
}

TEST(PythonOutput, HMap) {
  HeatmapPlot plot;
  plot.AddData({1, 2, 3, 4, 5});
  plot.AddData({1, 2, 3, 4, 5});
  plot.AddData({1, 2, 3, 4, 5});
  plot.AddData({1, 2, 3, 4, 5});
  plot.AddData({1, 2, 3, 4, 5});

  HtmlPage page;
  plot.PlotToHtml(&page);
  nc::File::WriteStringToFile(page.Construct(), "hmap.html");
}

TEST(PythonOutput, Npy) {
  std::vector<std::pair<std::string, NpyArray::FieldType>> fields;
  fields.emplace_back("e1", NpyArray::UINT64);
  fields.emplace_back("e2", NpyArray::DOUBLE);
  fields.emplace_back("e3", NpyArray::STRING);

  NpyArray npy_array(fields);
  npy_array.AddRow({34, 5.0, "a"});
  npy_array.ToDisk("npy_output_folder");
}

}  // namespace
}  // namespace grapher
}  // namespace nc
