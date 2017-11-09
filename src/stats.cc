#include "stats.h"

namespace nc {
void Bin(size_t bin_size, std::vector<std::pair<double, double>>* data) {
  CHECK(bin_size != 0);
  if (bin_size == 1) {
    return;
  }

  double bin_total = 0;
  size_t bin_index = 0;
  for (size_t i = 0; i < data->size(); ++i) {
    if (i != 0 && i % bin_size == 0) {
      double mean = bin_total / bin_size;
      double bin_start = (*data)[i - bin_size].first;
      (*data)[bin_index++] = {bin_start, mean};
      bin_total = 0;
    }

    bin_total += (*data)[i].second;
  }

  size_t remainder = data->size() % bin_size;
  size_t base = (data->size() / bin_size) * bin_size;
  if (remainder == 0) {
    data->resize(bin_index);
    return;
  }

  double mean = bin_total / remainder;
  double bin_start = (*data)[base].first;
  (*data)[bin_index++] = {bin_start, mean};
  data->resize(bin_index);
}

void Bin(size_t bin_size, std::vector<double>* data) {
  CHECK(bin_size != 0);
  if (bin_size == 1) {
    return;
  }

  double bin_total = 0;
  size_t bin_index = 0;
  for (size_t i = 0; i < data->size(); ++i) {
    if (i != 0 && i % bin_size == 0) {
      double mean = bin_total / bin_size;
      (*data)[bin_index++] = mean;
      bin_total = 0;
    }

    bin_total += (*data)[i];
  }

  size_t remainder = data->size() % bin_size;
  if (remainder == 0) {
    data->resize(bin_index);
    return;
  }

  double mean = bin_total / remainder;
  (*data)[bin_index++] = mean;
  data->resize(bin_index);
}

static double LinearInterpolate(double x0, double y0, double x1, double y1,
                                double x) {
  double a = (y1 - y0) / (x1 - x0);
  double b = -a * x0 + y0;
  double y = a * x + b;
  return y;
}

Empirical2DFunction::Empirical2DFunction(
    const std::vector<std::pair<double, double>>& values,
    Interpolation interpolation)
    : interpolation_type_(interpolation),
      low_fill_value_set_(false),
      low_fill_value_(0),
      high_fill_value_set_(false),
      high_fill_value_(0) {
  CHECK(!values.empty());
  for (const auto& x_and_y : values) {
    values_.emplace(x_and_y);
  }
}

Empirical2DFunction::Empirical2DFunction(const std::vector<double>& xs,
                                         const std::vector<double>& ys,
                                         Interpolation interpolation)
    : interpolation_type_(interpolation),
      low_fill_value_set_(false),
      low_fill_value_(0),
      high_fill_value_set_(false),
      high_fill_value_(0) {
  CHECK(!xs.empty());
  CHECK(xs.size() == ys.size());
  for (size_t i = 0; i < xs.size(); ++i) {
    double x = xs[i];
    double y = ys[i];
    values_.emplace(x, y);
  }
}

void Empirical2DFunction::SetLowFillValue(double value) {
  low_fill_value_set_ = true;
  low_fill_value_ = value;
}

void Empirical2DFunction::SetHighFillValue(double value) {
  high_fill_value_set_ = true;
  high_fill_value_ = value;
}

double Empirical2DFunction::Eval(double x) {
  auto lower_bound_it = values_.lower_bound(x);
  if (lower_bound_it == values_.begin()) {
    // x is below the data range.
    if (low_fill_value_set_) {
      return low_fill_value_;
    }

    return lower_bound_it->second;
  }

  if (lower_bound_it == values_.end()) {
    // x is above the data range.
    if (high_fill_value_set_) {
      return high_fill_value_;
    }

    // Need to get to the previous element.
    auto prev_it = std::prev(lower_bound_it);
    return prev_it->second;
  }

  if (lower_bound_it->first == x) {
    return lower_bound_it->second;
  }

  // The range that we ended up in is between the previous element of the
  // lower bound and the lower bound.
  auto prev_it = std::prev(lower_bound_it);

  double x0 = prev_it->first;
  double x1 = lower_bound_it->first;
  double y0 = prev_it->second;
  double y1 = lower_bound_it->second;

  CHECK(x0 <= x);
  CHECK(x1 >= x);
  if (interpolation_type_ == Interpolation::NEARERST) {
    double delta_one = x - x0;
    double delta_two = x1 - x;
    if (delta_one > delta_two) {
      return y1;
    }
    return y0;
  } else if (interpolation_type_ == Interpolation::LINEAR) {
    return LinearInterpolate(x0, y0, x1, y1, x);
  }

  LOG(FATAL) << "Bad interpolation type";
  return 0;
}

void SummaryStats::Add(double value) { AddCount(value, 1); }

void SummaryStats::AddCount(double value, size_t count) {
  static double max_add_value =
      std::pow(std::numeric_limits<double>::max(), 0.5);
  CHECK(value * count < max_add_value) << "Value too large " << value
                                       << " count " << count;

  double value_squared = value * value;
  if ((value_squared < 0.0) == (sum_squared_ < 0.0) &&
      std::abs(value_squared) >
          std::numeric_limits<double>::max() - std::abs(sum_squared_)) {
    LOG(FATAL) << "Addition overflowing";
  }

  if (value < min_) {
    min_ = value;
  }

  if (value > max_) {
    max_ = value;
  }

  count_ += count;
  sum_ += value * count;
  sum_squared_ += value_squared * count;
}

void SummaryStats::Reset() {
  sum_ = 0;
  count_ = 0;
  sum_squared_ = 0;
  min_ = std::numeric_limits<double>::max();
  max_ = std::numeric_limits<double>::min();
}

double SummaryStats::min() const {
  CHECK(count_ > 0) << "No values yet";
  return min_;
}

double SummaryStats::max() const {
  CHECK(count_ > 0) << "No values yet";
  return max_;
}

double SummaryStats::mean() const {
  CHECK(count_ > 0) << "No values yet";
  return sum_ / count_;
}

double SummaryStats::var() const {
  double m = mean();
  return sum_squared_ / count_ - m * m;
}

void SummaryStats::Reset(size_t count, double sum, double sum_squared,
                         double min, double max) {
  count_ = count;
  sum_ = sum;
  sum_squared_ = sum_squared;
  min_ = min;
  max_ = max;
}

}  // namespace nc
