// A memory-efficient column of numbers.

#ifndef NCODE_NUM_COL_H
#define NCODE_NUM_COL_H

#include <set>
#include <vector>
#include "common.h"
#include "interval_tree.h"
#include "stats.h"
#include "thread_runner.h"

namespace nc {

// A range of indices. The first value is the starting index, the second is the
// number of indices from the starting one. The range is empty if this value is
// equal to 0.
using Range = std::pair<uint32_t, uint32_t>;

// An immutable set of ranges.
class RangeSet {
 public:
  // Constructs a range set. Any overlap in the ranges argument will be
  // removed.
  explicit RangeSet(const std::vector<Range>& ranges,
                    bool already_sorted = false);

  // Same as above, but avoids an extra copy;
  explicit RangeSet(std::vector<Range>* ranges, bool already_sorted = false);

  // Returns a list of ranges.
  std::vector<Range> ranges() const { return ranges_; }

  bool operator==(const RangeSet& other) const {
    return ranges_ == other.ranges_;
  }

  // Checks if an index is contained in any of the ranges.
  bool ContainsIndex(uint32_t index) const {
    auto it = std::lower_bound(ranges_.begin(), ranges_.end(), index,
                               [](const Range& lhs, uint64_t rhs) {
                                 return (lhs.first + lhs.second - 1) < rhs;
                               });
    if (it == ranges_.end()) {
      return false;
    }

    return it->first <= index;
  }

 private:
  static void RemoveOverlap(std::vector<Range>* ranges);

  // Ranges, sorted and non-overlapping.
  std::vector<Range> ranges_;
};

template <typename T,
          typename =
              typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
class NumberVectorIndex {
 public:
  // Returns the indices of values in the range [from, to].
  virtual RangeSet IndicesOfValuesInRange(T from, T to) const = 0;

  // Estaimates the memory consumption of the index.
  virtual uint64_t ByteEstimate() const = 0;
};

template <typename T,
          typename =
              typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
class ImmutableNumberVector {
 public:
  virtual ~ImmutableNumberVector() {}

  // The value at a given index.
  virtual T ValueAt(uint32_t index) const = 0;

  // Number of values in the vector.
  virtual uint32_t size() const = 0;

  // Estaimates the memory consumption of the vector.
  virtual uint64_t ByteEstimate() const = 0;

  virtual T MinValue() const = 0;

  virtual T MaxValue() const = 0;
};

struct ImmutablePackedIntVectorStats {
  bool zig_zagged;
  uint8_t bytes_per_num;
  uint32_t num_values;
  uint64_t byte_size_estimate;
};

class ImmutablePackedIntVector : public ImmutableNumberVector<int64_t> {
 public:
  ImmutablePackedIntVector(const std::vector<int64_t>& values)
      : zig_zagged_(false) {
    std::vector<int64_t> values_copy = values;
    Init(&values_copy);
  }

  explicit ImmutablePackedIntVector(std::vector<int64_t>* values)
      : zig_zagged_(false) {
    Init(values);
  }

  // Returns the number of elements in the sequence.
  uint32_t size() const override { return data_.size() / bytes_per_num_; }

  // Returns the value at a given index.
  int64_t ValueAt(uint32_t index) const override;

  // Estaimates memory consumption.
  uint64_t ByteEstimate() const override;

  // Information about the vector.
  ImmutablePackedIntVectorStats Stats() const;

 private:
  static constexpr uint64_t kOneByteMask = (1UL << 8) - 1;
  static constexpr uint64_t kTwoByteMask = (1UL << 16) - 1;
  static constexpr uint64_t kThreeByteMask = (1UL << 24) - 1;
  static constexpr uint64_t kFourByteMask = (1UL << 32) - 1;
  static constexpr uint64_t kFiveByteMask = (1UL << 40) - 1;
  static constexpr uint64_t kSixByteMask = (1UL << 48) - 1;
  static constexpr uint64_t kSevenByteMask = (1UL << 56) - 1;

  // Encodes a series of values using ZigZag encoding.
  static void ZigZagEncode(std::vector<int64_t>* values);

  // Decodes a single ZigZag encoded value.
  static int64_t ZigZagDecode(uint64_t v);

  // Initializes the object. Should be called once, upon construction.
  void Init(std::vector<int64_t>* values);

  // Finds the minimal value and subtracts it from all values. Sets base_ to the
  // minimal value.
  void Rebase(std::vector<int64_t>* values);

  // Returns the raw (offset from base, potentially zigzag encoded) value.
  uint64_t RawValueAtIndex(uint32_t index) const;

  // All integers are stored as offsets from this value, which is the smallest
  // value in the sequence.
  uint64_t base_;

  // Number of bytes used to encode each integer. Fixed width.
  uint8_t bytes_per_num_;

  // If true values were pre-processed using zig-zag encoding to remove
  // negatives.
  bool zig_zagged_;

  // The actual data.
  std::vector<char> data_;
};

// Increasing or decreasing subsequence.
template <typename T>
class SortedSubsequence {
 public:
  static std::vector<SortedSubsequence<T>> Get(
      const ImmutableNumberVector<T>* v) {
    if (v->size() == 0) {
      return {};
    }

    Range current_range = {0, 1};
    bool is_increasing = true;
    bool is_increasing_set = false;

    std::vector<SortedSubsequence<T>> subsequences;
    for (size_t i = 0; i < v->size() - 1; ++i) {
      T this_value = v->ValueAt(i);
      T next_value = v->ValueAt(i + 1);

      if (!is_increasing_set) {
        is_increasing_set = true;
        is_increasing = this_value <= next_value;
        ++(current_range.second);
        continue;
      }

      if ((is_increasing && this_value <= next_value) ||
          (!is_increasing && this_value >= next_value)) {
        // We extend the current range.
        ++(current_range.second);
      } else {
        subsequences.emplace_back(current_range, is_increasing);
        is_increasing_set = false;
        current_range = {i + 1, 1};
      }
    }

    subsequences.emplace_back(current_range, is_increasing);
    return subsequences;
  }

  SortedSubsequence(const Range& range, bool increasing)
      : range_(range), increasing_(increasing) {
    CHECK(range.second != 0);
  }

  T MinValue(const ImmutableNumberVector<T>& v) const {
    return increasing_ ? v.ValueAt(range_.first)
                       : v.ValueAt(range_.first + range_.second - 1);
  }

  T MaxValue(const ImmutableNumberVector<T>& v) const {
    return increasing_ ? v.ValueAt(range_.first + range_.second - 1)
                       : v.ValueAt(range_.first);
  }

  uint32_t Bound(const ImmutableNumberVector<T>& v, T value,
                 bool lower = true) const {
    uint32_t count = range_.second;
    uint32_t threshold =
        increasing_ ? range_.first : range_.first + range_.second - 1;

    uint32_t it;
    int32_t step;
    while (count > 0) {
      it = threshold;
      step = count / 2;
      if (increasing_) {
        it += step;
      } else {
        it -= step;
      }

      if (lower ? v.ValueAt(it) < value : !(value < v.ValueAt(it))) {
        if (increasing_) {
          threshold = ++it;
        } else {
          threshold = --it;
        }

        count -= step + 1;
      } else {
        count = step;
      }
    }
    return threshold;
  }

  Range IndicesOfValuesInRange(const ImmutableNumberVector<T>& v, T from,
                               T to) const {
    CHECK(to >= from);
    uint32_t lower_from = Bound(v, from);
    uint32_t lower_to = Bound(v, to, false);

    if (increasing_) {
      return {lower_from, lower_to - lower_from};
    }
    return {lower_to + 1, lower_from - lower_to};
  }

  bool operator==(const SortedSubsequence<T>& other) const {
    return std::tie(range_, increasing_) ==
           std::tie(other.range_, other.increasing_);
  }

  const Range& range() const { return range_; }

  bool increasing() const { return increasing_; }

 private:
  // The range of indices covered by this subsequence.
  Range range_;

  // If true the subsequence is increasing, if false it is decreasing.
  bool increasing_;
};

// An index based on sorted subsequences of items.
template <typename T>
class SortedIntervalIndex : public NumberVectorIndex<T> {
 public:
  SortedIntervalIndex(const ImmutableNumberVector<T>* v)
      : v_(v), root_(GetIntervals(SortedSubsequence<T>::Get(v), *v)) {}

  // Returns the indices of values in the range [from, to].
  RangeSet IndicesOfValuesInRange(T from, T to) const override {
    std::vector<Range> ranges;
    root_.Lookup(
        from, to,
        [this, from, to, &ranges](const Interval& interval, uint32_t i) {
          Unused(i);
          const SortedSubsequence<T>& subsequence = interval.value;
          Range range = subsequence.IndicesOfValuesInRange(*v_, from, to);
          if (range.second) {
            ranges.emplace_back(range);
          }
        });

    return RangeSet(&ranges);
  }

  // Estaimates the memory consumption of the index.
  uint64_t ByteEstimate() const override {
    return root_.ByteEstimate() + sizeof(void*);
  }

 private:
  using Interval = interval_tree::Interval<T, SortedSubsequence<T>>;

  static std::vector<Interval> GetIntervals(
      const std::vector<SortedSubsequence<T>>& subsequences,
      const ImmutableNumberVector<T>& v) {
    std::vector<Interval> out;
    for (const auto& subsequence : subsequences) {
      out.push_back(
          {subsequence.MinValue(v), subsequence.MaxValue(v), subsequence});
    }

    return out;
  }

  const ImmutableNumberVector<T>* v_;
  interval_tree::TreeRoot<T, SortedSubsequence<T>> root_;
};

// A basic index that maintains a sorted copy of all values.
template <typename T>
class BasicIndex : public NumberVectorIndex<T> {
 public:
  BasicIndex(const ImmutableNumberVector<T>& v) {
    std::map<T, std::vector<uint32_t>> ranges;
    for (uint32_t i = 0; i < v.size(); ++i) {
      T value = v.ValueAt(i);
      ranges[value].emplace_back(i);
    }

    for (const auto& value_and_indices : ranges) {
      T value = value_and_indices.first;
      const std::vector<uint32_t>& indices = value_and_indices.second;

      std::vector<Range> ranges_for_value;
      for (uint32_t index : indices) {
        ranges_for_value.emplace_back(index, 1);
      }

      // The RangeSet will sort and combine the ranges.
      RangeSet range_set(ranges_for_value);
      for (const Range& range : range_set.ranges()) {
        values_.emplace_back(value, range);
      }
    }
    values_.shrink_to_fit();
  }

  uint64_t ByteEstimate() const override {
    return sizeof(std::pair<T, Range>) * values_.capacity();
  }

  RangeSet IndicesOfValuesInRange(T from, T to) const override {
    auto it = std::lower_bound(
        values_.begin(), values_.end(), from,
        [](const std::pair<T, Range>& lhs, T rhs) { return lhs.first < rhs; });

    std::vector<Range> ranges;
    if (it == values_.end()) {
      return RangeSet(ranges);
    }

    while (it != values_.end()) {
      if (it->first > to) {
        break;
      }

      Range range = it->second;
      ranges.emplace_back(range);
      ++it;
    }

    return RangeSet(ranges);
  }

 private:
  // For each value a range of indices that have that value. Sorted.
  std::vector<std::pair<T, Range>> values_;
};

template <typename T>
class NumberVector {
 public:
  // The value at a given index.
  T ValueAt(uint32_t index) const {
    uint32_t base = index / kChunkSize;
    uint32_t offset = index % kChunkSize;

    if (base == chunks_.size()) {
      return latest_[offset];
    }

    return chunks_[base].ValueAt(offset);
  }

  void Add(T value) {
    uint32_t index = latest_.size();
    latest_.emplace_back(value);
    latest_index_[value].emplace_back(index);

    if (latest_.size() == kChunkSize) {
      chunks_.emplace_back(latest_);
      latest_.clear();
      latest_index_.clear();
    }
  }

  // Number of values in the vector.
  uint32_t size() const = 0;

  // Estaimates the memory consumption of the vector.
  uint64_t ByteEstimate() const = 0;

  RangeSet IndicesOfValuesInRange(T from, T to) const = 0;

 private:
  static constexpr uint32_t kChunkSize = 1 << 14;

  struct ChunkAndIndex {
    ChunkAndIndex(const std::vector<T>& values) : chunk(values) {}

    void IndexChunk() {
      // Will pick the index that is more memory efficient.
      auto basic_index = make_unique<BasicIndex<T>>(chunk);
      auto interval_index = make_unique<SortedIntervalIndex<T>>(&chunk);

      if (basic_index->ByteEstimate() < interval_index->ByteEstimate()) {
        index = std::move(basic_index);
      } else {
        index = std::move(interval_index);
      }
    }

    RangeSet IndicesOfValuesInRange(T from, T to) {
      if (chunk.MinValue() > to || chunk.MaxValue() < from) {
        return {};
      }

      if (!index) {
        IndexChunk();
      }
      return index->IndicesOfValuesInRange(from, to);
    }

    ImmutableNumberVector<T> chunk;
    std::unique_ptr<NumberVectorIndex<T>> index;
  };

  std::vector<T> latest_;
  std::map<T, std::vector<uint32_t>> latest_index_;

  // The chunks.
  std::vector<ChunkAndIndex> chunks_;
};

}  // namespace nc

#endif
