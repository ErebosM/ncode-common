#ifndef NCODE_PREDICT_H
#define NCODE_PREDICT_H

#include <vector>
#include "common.h"

namespace nc {

// Given a series of values will predict the next one. Also keeps track of
// prediction error.
class Predictor {
 public:
  virtual ~Predictor() {}

  // Predicts next values based on previous values. Will predict 'steps' values
  // in the future. If steps is equal to 1 will predict the next value.
  double Predict(size_t steps);

  // Adds a new value.
  virtual void Add(double value) { values_.emplace_back(value); }

  // Returns for each value the error between its predicted value and real
  // value. The returned vector will have N-1 elements if there are N values and
  // steps is 1.
  std::vector<double> GetErrors(size_t steps);

 protected:
  // Predicts the next value using values up to (and including) 'index_to'.
  virtual double PredictNext(size_t index_to, size_t steps) = 0;

  std::vector<double> values_;
};

// A simple predictor that always predicts the previous value.
class DummyPredictor : public Predictor {
 protected:
  double PredictNext(size_t index_to, size_t steps) override {
    Unused(steps);
    return values_[index_to];
  }
};

class LinearLeastSquaresPredictor : public Predictor {
 public:
  LinearLeastSquaresPredictor(size_t window) : window_(window) {}

 protected:
  double PredictNext(size_t index_to, size_t steps) override;

 private:
  // Number of values in the "past" to base prediction on.
  size_t window_;
};

bool LinReg(std::vector<double>::const_iterator start_x,
            std::vector<double>::const_iterator end_x,
            std::vector<double>::const_iterator start_y,
            std::vector<double>::const_iterator end_y, double* m, double* b);

bool LinReg(std::vector<double>::const_iterator start_y,
            std::vector<double>::const_iterator end_y, double* m, double* b);

}  // namespace nc

#endif
