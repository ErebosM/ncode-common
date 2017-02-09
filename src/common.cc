#include "common.h"

#include <glob.h>
#include <string>

namespace nc {

std::vector<std::string> Glob(const std::string& pat) {
  glob_t glob_result;
  glob(pat.c_str(), GLOB_TILDE, NULL, &glob_result);
  std::vector<std::string> ret;
  for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
    ret.push_back(std::string(glob_result.gl_pathv[i]));
  }
  globfree(&glob_result);
  return ret;
}

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

uint32_t ghtonl(uint32_t x) {
  union {
    uint32_t result;
    uint8_t result_array[4];
  };
  result_array[0] = static_cast<uint8_t>(x >> 24);
  result_array[1] = static_cast<uint8_t>((x >> 16) & 0xFF);
  result_array[2] = static_cast<uint8_t>((x >> 8) & 0xFF);
  result_array[3] = static_cast<uint8_t>(x & 0xFF);
  return result;
}

void SummaryStats::Add(double value) {
  static double max_add_value =
      std::pow(std::numeric_limits<double>::max(), 0.5);
  CHECK(value < max_add_value) << "Value too large";

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

  ++count_;
  sum_ += value;
  sum_squared_ += value * value;
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

void ThresholdEnforcerPolicy::set_empty_threshold_absolute(
    double empty_threshold_absolute) {
  CHECK(empty_threshold_absolute >= 0)
      << "Absolute threshold should be a positive number";
  empty_threshold_absolute_ = empty_threshold_absolute;
}

void ThresholdEnforcerPolicy::set_threshold_absolute(
    double threshold_absolute) {
  CHECK(threshold_absolute >= 0)
      << "Absolute threshold should be a positive number";
  threshold_absolute_ = threshold_absolute;
}

void ThresholdEnforcerPolicy::set_threshold_relative_to_current(
    double threshold_relative_to_current) {
  CHECK(threshold_relative_to_current >= 0 &&
        threshold_relative_to_current <= 1)
      << "Relative threshold should be in [0-1]";
  threshold_relative_to_current_ = threshold_relative_to_current;
}

void ThresholdEnforcerPolicy::set_threshold_relative_to_total(
    double threshold_relative_to_total) {
  CHECK(threshold_relative_to_total >= 0 && threshold_relative_to_total <= 1)
      << "Relative threshold should be in [0-1]";
  threshold_relative_to_total_ = threshold_relative_to_total;
}

double ThresholdEnforcerPolicy::empty_threshold_absolute() const {
  return empty_threshold_absolute_;
}

double ThresholdEnforcerPolicy::threshold_absolute() const {
  return threshold_absolute_;
}

double ThresholdEnforcerPolicy::threshold_relative_to_current() const {
  return threshold_relative_to_current_;
}

double ThresholdEnforcerPolicy::threshold_relative_to_total() const {
  return threshold_relative_to_total_;
}

void TimeoutPolicy::set_timeout_penalty(uint64_t timeout_penalty) {
  timeout_penalty_ = timeout_penalty;
}

uint64_t TimeoutPolicy::timeout_penalty_lookback() const {
  return timeout_penalty_lookback_;
}

void TimeoutPolicy::set_timeout_penalty_lookback(
    uint64_t timeout_penalty_lookback) {
  timeout_penalty_lookback_ = timeout_penalty_lookback;
}

bool TimeoutPolicy::timeout_penalty_cumulative() const {
  return timeout_penalty_cumulative_;
}

void TimeoutPolicy::set_timeout_penalty_cumulative(
    bool timeout_penalty_cumulative) {
  timeout_penalty_cumulative_ = timeout_penalty_cumulative;
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

CountdownTimer::CountdownTimer(std::chrono::nanoseconds budget)
    : construction_time_(std::chrono::steady_clock::now()), budget_(budget) {}

bool CountdownTimer::Expired() const {
  using namespace std::chrono;
  auto now = steady_clock::now();
  nanoseconds delta = duration_cast<nanoseconds>(now - construction_time_);
  return delta >= budget_;
}

std::chrono::nanoseconds CountdownTimer::RemainingTime() const {
  using namespace std::chrono;
  auto now = steady_clock::now();
  nanoseconds delta = duration_cast<nanoseconds>(now - construction_time_);
  if (delta >= budget_) {
    return nanoseconds::zero();
  }
  return budget_ - delta;
}

std::string RandomString(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

}  // namespace nc
