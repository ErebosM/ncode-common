#include "gtest/gtest.h"
#include "predict.h"

namespace nc {
namespace {

class DummyPredictorTest : public ::testing::Test {
 protected:
  DummyPredictor predictor_;
};

TEST_F(DummyPredictorTest, Empty) { ASSERT_DEATH(predictor_.Predict(1), ".*"); }

TEST_F(DummyPredictorTest, SingleValue) {
  predictor_.Add(10);
  ASSERT_DEATH(predictor_.Predict(0), ".*");
  ASSERT_EQ(10.0, predictor_.Predict(1));
}

TEST_F(DummyPredictorTest, MultipleValues) {
  predictor_.Add(10);
  predictor_.Add(11);
  predictor_.Add(-10);
  ASSERT_EQ(-10.0, predictor_.Predict(1));
}

TEST_F(DummyPredictorTest, SingleError) {
  predictor_.Add(10);

  std::vector<double> model;
  ASSERT_EQ(model, predictor_.GetErrors(1));
}

TEST_F(DummyPredictorTest, MultiError) {
  predictor_.Add(10);
  predictor_.Add(10);
  predictor_.Add(11);

  std::vector<double> model = {0.0, 1.0 / 11.0};
  ASSERT_EQ(model, predictor_.GetErrors(1));
}

class LinPredictorTest : public ::testing::Test {
 protected:
  LinPredictorTest() : predictor_(60) {}

  LinearLeastSquaresPredictor predictor_;
};

TEST_F(LinPredictorTest, OneValue) {
  predictor_.Add(1);
  ASSERT_EQ(1, predictor_.Predict(1));
  ASSERT_EQ(1, predictor_.Predict(1));
}

TEST_F(LinPredictorTest, TwoValues) {
  predictor_.Add(1);
  predictor_.Add(2);

  ASSERT_EQ(3, predictor_.Predict(1));
  ASSERT_EQ(4, predictor_.Predict(2));
  ASSERT_EQ(102, predictor_.Predict(100));

  std::vector<double> model = {0.5};
  ASSERT_EQ(model, predictor_.GetErrors(1));

  model.clear();
  ASSERT_EQ(model, predictor_.GetErrors(2));
}

TEST_F(LinPredictorTest, MultiValues) {
  for (size_t i = 0; i < 10; ++i) {
    predictor_.Add(i);
  }

  ASSERT_EQ(10, predictor_.Predict(1));
  ASSERT_EQ(11, predictor_.Predict(2));

  // The first error is 1 because based only on the first element (0) the
  // prediction will be 0, which is 1 away from the real next value of 1.
  std::vector<double> model = {1, 0, 0, 0, 0, 0, 0, 0, 0};
  ASSERT_EQ(model, predictor_.GetErrors(1));

  model.pop_back();
  ASSERT_EQ(model, predictor_.GetErrors(2));
}

TEST_F(LinPredictorTest, Window) {
  for (size_t i = 0; i < 100; ++i) {
    predictor_.Add(0);
  }

  for (size_t i = 0; i < 60; ++i) {
    predictor_.Add(i);
  }

  // Prediction should only be based on the last window of values.
  ASSERT_EQ(60, predictor_.Predict(1));
  ASSERT_EQ(61, predictor_.Predict(2));
}

}  // namespace
}  // namespace nc
