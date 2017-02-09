#ifndef NCODE_CIRCULAR_ARRAY_H_
#define NCODE_CIRCULAR_ARRAY_H_

#include <array>
#include <cstdbool>
#include <vector>

#include "common.h"
#include "logging.h"

namespace nc {

// A simple circular array.
template <typename T, size_t NumValues>
class CircularArray {
 public:
  static constexpr size_t kMaxValues = NumValues;

  CircularArray() : num_values_(0), index_(0) {
    static_assert(IsPowerOfTwo(NumValues),
                  "Number of values should be power of 2");
  }

  // Adds a new value to this array.
  void AddValue(T value) {
    values_[index_++ & kMask] = value;
    if (num_values_ < NumValues) {
      num_values_++;
    }
  }

  // Moves a value into this array.
  void MoveValue(T&& value) {
    values_[index_++ & kMask] = std::move(value);
    if (num_values_ < NumValues) {
      num_values_++;
    }
  }

  // Number of elements in the array.
  size_t size() const { return num_values_; }

  // Returns true if the array has no values.
  bool empty() const { return num_values_ == 0; }

  // Returns a const reference to the most recently inserted value in the array
  // or dies if the array is empty.
  const T& MostRecentValueOrDie() const {
    CHECK(num_values_ > 0) << "Circular array empty";
    return values_[(index_ - 1) & kMask];
  }

  // Returns a const reference to the value that has spent the most time in the
  // array.
  const T& OldestValueOrDie() const {
    CHECK(num_values_ > 0) << "Circular array empty";
    size_t start = (index_ - num_values_) & kMask;
    return values_[start];
  }

  // Returns a vector with all values in this array, in insertion order. After
  // this call the array will be empty.
  std::vector<T> GetValues() {
    size_t start = (index_ - num_values_) & kMask;
    std::vector<T> values;
    values.reserve(num_values_);
    for (size_t i = 0; i < num_values_; ++i) {
      size_t value_index = (start + i) & kMask;
      values.emplace_back(std::move(values_[value_index]));
    }

    num_values_ = 0;
    index_ = 0;
    return values;
  }

 private:
  static constexpr size_t kMask = NumValues - 1;

  size_t num_values_;
  size_t index_;
  std::array<T, NumValues> values_;

  DISALLOW_COPY_AND_ASSIGN(CircularArray);
};

}  // namespace nc

#endif
