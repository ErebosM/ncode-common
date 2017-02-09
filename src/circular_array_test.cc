#include "circular_array.h"

#include "common.h"
#include "map_util.h"
#include "gtest/gtest.h"

namespace nc {
namespace {

static constexpr size_t kWindow = 1024;

class CircularArrayOneFixture : public ::testing::Test {
 protected:
  CircularArrayOneFixture() : model_entry_(1.0) {}

  CircularArray<double, 1> array_;
  double model_entry_;
};

TEST_F(CircularArrayOneFixture, Empty) {
  ASSERT_EQ(0ul, array_.size());

  std::vector<double> values = array_.GetValues();
  ASSERT_TRUE(values.empty());
}

TEST_F(CircularArrayOneFixture, AddOne) {
  array_.AddValue(model_entry_);
  ASSERT_EQ(1ul, array_.size());
}

TEST_F(CircularArrayOneFixture, AddOneMostRecent) {
  array_.AddValue(model_entry_);

  ASSERT_EQ(model_entry_, array_.MostRecentValueOrDie());
  ASSERT_EQ(model_entry_, array_.MostRecentValueOrDie());
}

TEST_F(CircularArrayOneFixture, AddOneValues) {
  array_.AddValue(model_entry_);
  std::vector<double> values = array_.GetValues();

  std::vector<double> model = {model_entry_};
  ASSERT_EQ(model, values);
  ASSERT_TRUE(array_.empty());
}

TEST_F(CircularArrayOneFixture, AddMulti) {
  std::vector<double> values;
  for (double i = 0; i < 1000; ++i) {
    array_.AddValue(i);

    ASSERT_EQ(1ul, array_.size());
    ASSERT_EQ(i, array_.MostRecentValueOrDie());

    std::vector<double> model = {i};
    ASSERT_EQ(model, array_.GetValues());
  }
}

class CircularArrayFixture : public ::testing::Test {
 protected:
  CircularArray<double, kWindow> array_;
};

TEST_F(CircularArrayFixture, AddMultiFit) {
  std::vector<double> model_values;

  for (double i = 0; i < kWindow; ++i) {
    array_.AddValue(i);
    model_values.emplace_back(i);

    ASSERT_EQ(i, array_.MostRecentValueOrDie());
    ASSERT_EQ(0, array_.OldestValueOrDie());
  }

  ASSERT_EQ(model_values, array_.GetValues());
}

TEST_F(CircularArrayFixture, AddMultiNoFit) {
  std::vector<double> model_values;

  for (double i = 0; i < 5 * kWindow; ++i) {
    array_.AddValue(i);
    model_values.emplace_back(i);

    ASSERT_EQ(i, array_.MostRecentValueOrDie());
  }

  std::vector<double> values = array_.GetValues();
  ASSERT_EQ(kWindow, values.size());
  for (size_t i = 0; i < kWindow; ++i) {
    size_t index = model_values.size() - kWindow + i;
    ASSERT_EQ(model_values[index], values[i]);
  }
}

TEST(CircularArray, Movable) {
  CircularArray<std::unique_ptr<int>, kWindow> array;

  auto new_value = make_unique<int>(10);
  array.MoveValue(std::move(new_value));
  ASSERT_EQ(1ul, array.size());
  ASSERT_EQ(10, *array.MostRecentValueOrDie());

  std::vector<std::unique_ptr<int>> values = array.GetValues();
  ASSERT_EQ(1ul, values.size());
  ASSERT_EQ(10, *values.front());
}

}  // namespace
}  // namespace nc
