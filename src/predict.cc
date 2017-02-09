#include "predict.h"

#include <stddef.h>
#include <iterator>
#include <numeric>
#include <string>

#include "logging.h"
#include "strutil.h"

namespace nc {

double Predictor::Predict(size_t steps) {
  CHECK(steps > 0);
  CHECK(!values_.empty());
  return PredictNext(values_.size() - 1, steps);
}

bool LinReg(std::vector<double>::const_iterator start_y,
            std::vector<double>::const_iterator end_y, double* m, double* b) {
  size_t size = std::distance(start_y, end_y);
  std::vector<double> x(size);
  std::iota(x.begin(), x.end(), 0.0);
  return LinReg(x.begin(), x.end(), start_y, end_y, m, b);
}

bool LinReg(std::vector<double>::const_iterator start_x,
            std::vector<double>::const_iterator end_x,
            std::vector<double>::const_iterator start_y,
            std::vector<double>::const_iterator end_y, double* m, double* b) {
  double sumx = 0.0;  /* sum of x                      */
  double sumx2 = 0.0; /* sum of x**2                   */
  double sumxy = 0.0; /* sum of x * y                  */
  double sumy = 0.0;  /* sum of y                      */
  double sumy2 = 0.0; /* sum of y**2                   */

  auto size = std::distance(start_x, end_x);
  CHECK(std::distance(start_y, end_y) == size);

  for (size_t i = 0; i < static_cast<size_t>(size); i++) {
    double x = *std::next(start_x, i);
    double y = *std::next(start_y, i);

    sumx += x;
    sumx2 += x * x;
    sumxy += x * y;
    sumy += y;
    sumy2 += y * y;
  }

  double denom = (size * sumx2 - sumx * sumx);
  if (denom == 0) {
    // Singular matrix. Can't solve the problem.
    *m = 0;
    *b = 0;
    return false;
  }

  *m = (size * sumxy - sumx * sumy) / denom;
  *b = (sumy * sumx2 - sumx * sumxy) / denom;
  return true;
}

std::vector<double> Predictor::GetErrors(size_t steps) {
  std::vector<double> errors;
  for (size_t i = 0; i < values_.size(); ++i) {
    size_t next_index = i + steps;
    if (values_.size() <= next_index) {
      break;
    }

    double prediction = PredictNext(i, steps);
    double true_value = values_[next_index];
    double error = (true_value - prediction) / true_value;
    errors.emplace_back(error);
  }

  return errors;
}

double LinearLeastSquaresPredictor::PredictNext(size_t index_to, size_t steps) {
  size_t index = index_to + 1;
  size_t start = index > window_ ? index - window_ : 0;
  std::vector<double> y(std::next(values_.begin(), start),
                        std::next(values_.begin(), index));
  std::vector<double> x(y.size());
  std::iota(x.begin(), x.end(), 0.0);

  double m;
  double b;
  if (!LinReg(x.begin(), x.end(), y.begin(), y.end(), &m, &b)) {
    return values_[index_to];
  }

  return m * (x.back() + steps) + b;
}

}  // namespace nc
