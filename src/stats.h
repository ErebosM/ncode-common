#ifndef NCODE_STATS_H
#define NCODE_STATS_H

#include <vector>
#include "common.h"

namespace nc {

// Same as Percentiles below, but a callback is used to extract the order of
// values.
template <typename T, typename Compare>
std::vector<T> PercentilesWithCallback(std::vector<T>* values, Compare compare,
                                       size_t n = 100) {
  if (values == nullptr || values->empty()) {
    return std::vector<T>();
  }

  std::sort(values->begin(), values->end(), compare);
  double num_values_min_one = values->size() - 1;

  std::vector<T> return_vector(n + 1);
  for (size_t percentile = 0; percentile < n + 1; ++percentile) {
    size_t index =
        0.5 + num_values_min_one * (percentile / static_cast<double>(n));
    return_vector[percentile] = (*values)[index];
  }

  return return_vector;
}

// Returns a vector with n+1 values, each of which correcsponds to the i-th
// percentile of the distribution of an input vector of values. The first
// element (at index 0) is the minimum value (the "0th" percentile). The type T
// should be comparable (since the values array will be sorted).
template <typename T>
std::vector<T> Percentiles(std::vector<T>* values, size_t n = 100) {
  return PercentilesWithCallback(
      values, [](const T& lhs, const T& rhs) { return lhs < rhs; }, n);
}

// Bins a vector of x,y points such that each 'bin_size' points are replaced by
// a single point. The x value of the point will be the first x value in the bin
// and the y value will be the mean of all y values in the bin.
void Bin(size_t bin_size, std::vector<std::pair<double, double>>* data);

// Basic statistics about a series of numbers.
class SummaryStats {
 public:
  SummaryStats() { Reset(); }

  void Add(double value);

  // Like calling Add count times.
  void AddCount(double value, size_t count);

  size_t count() const { return count_; };

  double mean() const;

  double var() const;

  double std() const { return std::pow(var(), 0.5); }

  double min() const;

  double max() const;

  double sum() const { return sum_; }

  double sum_squared() const { return sum_squared_; }

  // Resets this SummaryStats to its original state (no values).
  void Reset();

  // Resets the internal state of the SummaryStats to the given values.
  void Reset(size_t count, double sum, double sum_squared, double min,
             double max);

 private:
  double sum_;
  size_t count_;
  double sum_squared_;
  double min_;
  double max_;
};

// A 2 dimensional empirical function. Can be used to interpolate values.
class Empirical2DFunction {
 public:
  // The type of interpolation to use.
  enum Interpolation {
    NEARERST = 0,
    LINEAR = 1,
  };

  Empirical2DFunction(const std::vector<std::pair<double, double>>& values,
                      Interpolation interpolation);
  Empirical2DFunction(const std::vector<double>& xs,
                      const std::vector<double>& ys,
                      Interpolation interpolation);

  // Value to be returned by Eval for x which is less than the data point with
  // min x. If this is not called the closest point is always returned when
  // extrapolating below the data range.
  void SetLowFillValue(double value);

  // Value to be returned by Eval for x which is more than the data point with
  // max x. If this is not called the closest point is always returned when
  // extrapolating above the data range.
  void SetHighFillValue(double value);

  // Returns the Y for a given X. If x is not in the original points the value
  // of Y will be interpolated. If x is outside the domain of
  double Eval(double x);

 private:
  Interpolation interpolation_type_;

  bool low_fill_value_set_;
  double low_fill_value_;
  bool high_fill_value_set_;
  double high_fill_value_;

  std::map<double, double> values_;
};

// Basic distribution information about a series of numbers.
template <typename T>
class DiscreteDistribution {
 public:
  static_assert(std::is_integral<T>::value, "Need an integral type");
  DiscreteDistribution() {}

  DiscreteDistribution(const std::vector<T>& values) {
    for (T value : values) {
      summary_stats_.Add(value);
      ++counts_[value];
    }
  }

  void Add(T value, size_t count) {
    summary_stats_.AddCount(value, count);
    counts_[value] += count;
  }

  std::map<T, double> Probabilities() const {
    std::map<T, double> out;

    for (const auto& value_and_count : counts_) {
      T value = value_and_count.first;
      double count = value_and_count.second;
      out[value] = count / summary_stats_.count();
    }

    return out;
  }

  // Returns the percentiles of the distribution. Value of ps should be in [0,
  // 1], and ps should be sorted.
  T Percentile(double p) const {
    size_t limit = p * summary_stats_.count();
    size_t total = 0;

    for (const auto& value_and_count : counts_) {
      T value = value_and_count.first;
      uint64_t count = value_and_count.second;

      total += count;
      if (total >= limit) {
        return value;
      }
    }

    LOG(FATAL) << "Should not happen";
    return T();
  }

  // Returns the percentiles of this distribution.
  std::vector<T> Percentiles(size_t percentile_count = 100) const {
    std::vector<T> out;
    for (size_t i = 0; i < percentile_count + 1; ++i) {
      double p = static_cast<double>(i) / percentile_count;
      out.emplace_back(Percentile(p));
    }

    return out;
  }

  const SummaryStats& summary_stats() const { return summary_stats_; }

 private:
  SummaryStats summary_stats_;
  std::map<T, uint64_t> counts_;
};

// Returns the sum of two empirical distributions.
template <typename T>
std::map<T, double> SumConvolute(const std::map<T, double>& x_probabilities,
                                 const std::map<T, double>& y_probabilities) {
  T max_x = std::prev(x_probabilities.end())->first;
  T max_y = std::prev(y_probabilities.end())->first;
  T min_x = x_probabilities.begin()->first;
  T min_y = y_probabilities.begin()->first;

  T from = std::min(min_x, min_y);
  T to = max_x + max_y;

  std::map<T, double> out;
  for (T value = from; value <= to; ++value) {
    double total = 0;
    for (const auto& value_and_prob : x_probabilities) {
      T x_value = value_and_prob.first;
      T y_value = value - x_value;
      const double* y_prob = FindOrNull(y_probabilities, y_value);
      if (y_prob == nullptr) {
        continue;
      }

      double x_prob = value_and_prob.second;
      total += x_prob * (*y_prob);
    }

    if (total > 0) {
      out[value] = total;
    }
  }

  return out;
}

template <typename T>
std::map<T, double> SumConvolute(
    const std::vector<std::map<T, double>>& probabilities) {
  CHECK(!probabilities.empty());
  auto it = probabilities.begin();

  std::map<T, double>& current_map = *it;
  ++it;

  for (; it != probabilities.end(); ++it) {
    current_map = SumConvolute(current_map, *it);
  }

  return current_map;
}

}  // namespace nc

#endif
