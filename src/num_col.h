// A memory-efficient column of numbers.

#ifndef NCODE_NUM_COL_H
#define NCODE_NUM_COL_H

#include <set>
#include <vector>
#include "common.h"
#include "interval_tree.h"
#include "packer.h"
#include "stats.h"
#include "thread_runner.h"

namespace nc {

// A range of indices. The first value is the starting index, the second is the
// number of indices from the starting one. The range is empty if this value is
// equal to 0.
template <typename I = size_t>
using Range = std::pair<I, I>;

// An immutable set of ranges.
template <typename I = size_t>
class RangeSet {
 public:
  RangeSet() {}

  // Constructs a range set. Any overlap in the ranges argument will be
  // removed.
  explicit RangeSet(const std::vector<Range<I>>& ranges,
                    bool already_sorted = false);

  // Same as above, but avoids an extra copy;
  explicit RangeSet(std::vector<Range<I>>* ranges, bool already_sorted = false);

  // Offsets all ranges in this set.
  void Offset(I offset);

  // Returns the number of elements covered by all ranges in this set.
  uint64_t ElementCount() const;

  // Returns a list of ranges.
  const std::vector<Range<I>>& ranges() const { return ranges_; }

  // Checks if an index is contained in any of the ranges.
  bool ContainsIndex(I index) const;

  // Pretty-prints this set of ranges.
  std::string ToString() const;

  bool operator==(const RangeSet<I>& other) const {
    return ranges_ == other.ranges_;
  }

 private:
  static void RemoveOverlap(std::vector<Range<I>>* ranges);

  // Ranges, sorted and non-overlapping.
  std::vector<Range<I>> ranges_;
};

struct ImmutablePackedIntVectorStats {
  bool zig_zagged;
  uint8_t bytes_per_num;
  uint32_t num_values;
  uint64_t byte_size_estimate;
};

class ImmutablePackedIntVector {
 public:
  using value_type = int64_t;

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
  size_t size() const { return data_.size() / bytes_per_num_; }

  // Returns the value at a given index.
  int64_t at(size_t index) const;

  // Estaimates memory consumption.
  uint64_t ByteEstimate() const;

  // Information about the vector.
  ImmutablePackedIntVectorStats Stats() const;

  int64_t min_value() const { return min_; }

  int64_t max_value() const { return max_; }

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
  uint64_t RawValueAtIndex(size_t index) const;

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

  // Max/min values.
  int64_t min_;
  int64_t max_;

  DISALLOW_COPY_AND_ASSIGN(ImmutablePackedIntVector);
};

// Increasing or decreasing subsequence.
template <typename I = size_t>
class SortedSubsequence {
 public:
  // Constructs a vector of SortedSubsequences out of all values in a container.
  template <typename Container>
  static std::vector<SortedSubsequence> Get(const Container& container);

  SortedSubsequence(const Range<I>& range, bool increasing)
      : range_(range), increasing_(increasing) {
    CHECK(range.second != 0);
  }

  template <typename Container>
  typename Container::value_type MinValue(const Container& container) const {
    return increasing_ ? container.at(range_.first)
                       : container.at(range_.first + range_.second - 1);
  }

  template <typename Container>
  typename Container::value_type MaxValue(const Container& container) const {
    return increasing_ ? container.at(range_.first + range_.second - 1)
                       : container.at(range_.first);
  }

  // Performs a binary search to find the index of the upper (or lower) bound of
  // a value.
  template <typename Container>
  I Bound(const Container& container, typename Container::value_type value,
          bool lower = true) const;

  // Returns a portion of this subsequence for which all values are between two
  // given values.
  template <typename Container>
  Range<I> IndicesOfValuesInRange(const Container& container,
                                  typename Container::value_type from,
                                  typename Container::value_type to) const;

  bool operator==(const SortedSubsequence& other) const {
    return std::tie(range_, increasing_) ==
           std::tie(other.range_, other.increasing_);
  }

  const Range<I>& range() const { return range_; }

  bool increasing() const { return increasing_; }

 private:
  // The range of indices covered by this subsequence.
  Range<I> range_;

  // If true the subsequence is increasing, if false it is decreasing.
  bool increasing_;
};

// An index based on sorted subsequences of items.
template <typename Container, typename I = size_t>
class SortedIntervalIndex {
 public:
  using T = typename Container::value_type;

  SortedIntervalIndex(const Container* container)
      : container_(container),
        root_(GetIntervals(
            SortedSubsequence<I>::template Get<Container>(*container),
            *container)) {}

  // Returns the indices of values in the range [from, to]. Type of ConsumerF is
  // bool(const Range&).
  template <typename ConsumerF>
  void ConsumeRanges(T from, T to, ConsumerF consumer) const;

  // Estaimates the memory consumption of the index.
  uint64_t ByteEstimate() const { return root_.ByteEstimate() + sizeof(void*); }

 private:
  using Interval = interval_tree::Interval<T, SortedSubsequence<I>>;

  // Helper function for construction.
  static std::vector<Interval> GetIntervals(
      const std::vector<SortedSubsequence<I>>& subsequences,
      const Container& container);

  const Container* container_;
  interval_tree::TreeRoot<T, SortedSubsequence<I>> root_;

  DISALLOW_COPY_AND_ASSIGN(SortedIntervalIndex);
};

// A basic index that maintains a sorted copy of all values.
template <typename T, typename I = size_t>
class BasicIndex {
 public:
  template <typename Container>
  BasicIndex(const Container& container) {
    CHECK(container.size() <= std::numeric_limits<I>::max());
    std::map<T, std::vector<I>> ranges;
    for (I i = 0; i < container.size(); ++i) {
      T value = container.at(i);
      ranges[value].emplace_back(i);
    }

    for (const auto& value_and_indices : ranges) {
      T value = value_and_indices.first;
      const std::vector<I>& indices = value_and_indices.second;

      std::vector<Range<I>> ranges_for_value;
      for (I index : indices) {
        ranges_for_value.emplace_back(index, 1);
      }

      // The RangeSet will sort and combine the ranges.
      RangeSet<I> range_set(ranges_for_value);
      for (const Range<I>& range : range_set.ranges()) {
        values_.emplace_back(value, range);
      }
    }
    values_.shrink_to_fit();
  }

  uint64_t ByteEstimate() const {
    return sizeof(std::pair<T, Range<I>>) * values_.capacity();
  }

  // Consumes ranges one at a time. Type of ConsumerF is bool(const Range&).
  template <typename ConsumerF>
  void ConsumeRanges(T from, T to, ConsumerF consumer) const {
    auto it = std::lower_bound(values_.begin(), values_.end(), from,
                               [](const std::pair<T, Range<I>>& lhs, T rhs) {
                                 return lhs.first < rhs;
                               });
    if (it == values_.end()) {
      return;
    }

    while (it != values_.end()) {
      if (it->first > to) {
        break;
      }

      Range<I> range = it->second;
      if (!consumer(range)) {
        break;
      }
      ++it;
    }
  }

 private:
  // For each value a range of indices that have that value. Sorted.
  std::vector<std::pair<T, Range<I>>> values_;

  DISALLOW_COPY_AND_ASSIGN(BasicIndex);
};

enum StorageType {
  // Packed integer values.
  INT_PACKED,
  // Run-length encoded values.template
  RLE,
  // A bit vector.
  BIT_VECTOR
};

enum IndexType {
  // Not indexed yet.
  UNINDEXED,
  // Basic sorted index.
  BASIC,
  // Sorted interval index.
  SORTED_INTERVAL,
  // True/false ranges, used on bool storage.
  BIT_RANGES
};

std::string StorageTypeToString(StorageType storage_type);
std::string IndexTypeToString(IndexType index_type);
std::string BytesToString(uint64_t bytes);
std::string NumericalQuantityToString(uint64_t value);

template <typename I>
class IntegerStorageChunk {
 public:
  using IType = I;

  IntegerStorageChunk(const std::vector<int64_t>& values);

  int64_t at(I index) const;

  I size() const;

  int64_t MinValue() const;

  int64_t MaxValue() const;

  uint64_t StorageByteEstimate() const;

  uint64_t IndexByteEstimate() const;

  IndexType IndexType() const;

  StorageType StorageType() const;

  template <typename ConsumerF>
  void ConsumeRanges(int64_t from, int64_t to, ConsumerF consumer);

 private:
  void Index();

  // Only one of those two will be set.
  std::unique_ptr<ImmutablePackedIntVector> packed_int_vector_;
  std::unique_ptr<RLEField<int64_t>> rle_;

  // Indices, only one will be set if the storage is indexed at all. Protected
  // by a mutex.
  std::unique_ptr<BasicIndex<int64_t, I>> basic_index_;
  std::unique_ptr<SortedIntervalIndex<ImmutablePackedIntVector, I>> packed_index_;
  std::unique_ptr<SortedIntervalIndex<RLEField<int64_t>>> rle_index_;

  bool indexed_;
  mutable std::mutex index_mutex_;
};

template <typename I>
class BoolStorageChunk {
 public:
  using IType = I;

  BoolStorageChunk(const std::vector<bool>& values)
      : bool_vector_(make_unique<std::vector<bool>>(values)),
        rle_(make_unique<RLEField<bool>>(values)),
        indexed_(false) {
    uint64_t bool_vector_bytes_estimate = bool_vector_->capacity() / 8;
    if (bool_vector_bytes_estimate > rle_->ByteEstimate()) {
      bool_vector_.reset();
    } else {
      rle_.reset();
    }
  }

  bool at(I index) const {
    if (bool_vector_) {
      return (*bool_vector_)[index];
    }

    if (rle_) {
      return rle_->at(index);
    }

    LOG(FATAL) << "Storage not set";
    return false;
  }

  I size() const {
    if (bool_vector_) {
      return bool_vector_->size();
    }

    if (rle_) {
      return rle_->size();
    }

    LOG(FATAL) << "Storage not set";
    return 0;
  }

  uint64_t StorageByteEstimate() const {
    if (bool_vector_) {
      return bool_vector_->size() / 8;
    }

    if (rle_) {
      return rle_->ByteEstimate();
    }

    LOG(FATAL) << "Storage not set";
    return 0;
  }

  uint64_t IndexByteEstimate() const {
    std::lock_guard<std::mutex> lock(index_mutex_);

    return true_ranges_.capacity() * sizeof(Range<I>) +
           false_ranges_.capacity() * sizeof(Range<I>);
  }

  IndexType IndexType() const {
    std::lock_guard<std::mutex> lock(index_mutex_);
    if (!indexed_) {
      return UNINDEXED;
    }
    return BIT_RANGES;
  }

  StorageType StorageType() const {
    if (bool_vector_) {
      return BIT_VECTOR;
    }

    if (rle_) {
      return RLE;
    }

    LOG(FATAL) << "Storage not set";
    return BIT_VECTOR;
  }

  template <typename ConsumerF>
  void ConsumeRanges(bool from, bool to, ConsumerF consumer) {
    CHECK(to >= from);
    Index();

    if (from == false) {
      for (const auto& range : false_ranges_) {
        consumer(range);
      }
    }

    if (to == true) {
      for (const auto& range : true_ranges_) {
        consumer(range);
      }
    }
  }

 private:
  void Index() {
    std::lock_guard<std::mutex> lock(index_mutex_);

    if (indexed_) {
      return;
    }

    std::vector<Range<I>> true_indices;
    std::vector<Range<I>> false_indices;

    uint32_t num_elements = size();
    for (uint32_t i = 0; i < num_elements; ++i) {
      if (at(i)) {
        true_indices.emplace_back(i, 1);
      } else {
        false_indices.emplace_back(i, 1);
      }
    }

    true_ranges_ = RangeSet<I>(true_indices).ranges();
    false_ranges_ = RangeSet<I>(false_indices).ranges();

    indexed_ = true;
  }

  // Only one of those two will be set.
  std::unique_ptr<std::vector<bool>> bool_vector_;
  std::unique_ptr<RLEField<bool>> rle_;

  // The index is the set of true/false ranges.
  std::vector<Range<I>> true_ranges_;
  std::vector<Range<I>> false_ranges_;

  bool indexed_;
  mutable std::mutex index_mutex_;
};

template <typename T, typename ChunkStorageType>
class Storage {
 public:
  // The value at a given index.
  int64_t at(size_t index) const {
    size_t base = index / kChunkSize;
    size_t offset = index % kChunkSize;

    if (base == chunks_.size()) {
      return latest_[offset];
    }

    return chunks_[base]->at(offset);
  }

  void Add(T value) {
    size_t index = latest_.size();
    latest_.push_back(value);
    latest_index_[value].emplace_back(index);

    if (latest_.size() == kChunkSize) {
      chunks_.emplace_back(make_unique<ChunkStorageType>(latest_));
      latest_.clear();
      latest_index_.clear();
    }
  }

  // Number of values in the vector.
  size_t size() const { return latest_.size() + kChunkSize * chunks_.size(); }

  template <typename ConsumerF>
  void ConsumeRanges(T from, T to, ConsumerF consumer) {
    ConsumeRangesFromLatest(from, to, consumer);
    for (size_t i = 0; i < chunks_.size(); ++i) {
      chunks_[i]->ConsumeRanges(from, to, [consumer, i](const Range<I>& range) {
        Range<size_t> range_cpy = {range.first + i * kChunkSize, range.second};
        return consumer(range_cpy);
      });
    }
  }

  static std::string DistBytes(const std::vector<uint64_t>& bytes) {
    std::vector<uint64_t> bytes_copy = bytes;
    std::vector<uint64_t> p = Percentiles(&bytes_copy);
    return Substitute("(min $0, med $1, 90p $2, max $3)", BytesToString(p[0]),
                      BytesToString(p[50]), BytesToString(p[90]),
                      BytesToString(p[100]));
  }

  std::string ToString() const {
    std::map<StorageType, std::vector<uint64_t>> storage_sizes;
    std::map<StorageType, std::map<IndexType, std::vector<uint64_t>>>
        index_sizes;
    for (const auto& chunk : chunks_) {
      uint64_t storage_size_bytes = chunk->StorageByteEstimate();
      StorageType storage_type = chunk->StorageType();
      uint64_t index_size_bytes = chunk->IndexByteEstimate();
      IndexType index_type = chunk->IndexType();

      storage_sizes[storage_type].emplace_back(storage_size_bytes);
      index_sizes[storage_type][index_type].emplace_back(index_size_bytes);
    }

    std::string out =
        Substitute("$0 chunks, with $1 elements each ($2 total)\n",
                   NumericalQuantityToString(chunks_.size()),
                   NumericalQuantityToString(kChunkSize),
                   NumericalQuantityToString(chunks_.size() * kChunkSize));
    for (const auto& storage_and_sizes : storage_sizes) {
      StorageType storage_type = storage_and_sizes.first;
      const std::vector<uint64_t>& sizes = storage_and_sizes.second;
      uint64_t total_storage_bytes =
          std::accumulate(sizes.begin(), sizes.end(), 0ul);

      StrAppend(&out, Substitute("\t$0: $1 elements, $2 $3\n",
                                 StorageTypeToString(storage_type),
                                 NumericalQuantityToString(sizes.size()),
                                 BytesToString(total_storage_bytes),
                                 DistBytes(sizes)));

      const std::map<IndexType, std::vector<uint64_t>>& index_map =
          FindOrDie(index_sizes, storage_type);
      for (const auto& index_and_sizes : index_map) {
        IndexType index_type = index_and_sizes.first;
        const std::vector<uint64_t>& sizes_for_index = index_and_sizes.second;
        if (index_type == UNINDEXED) {
          StrAppend(
              &out,
              Substitute("\t\t$0 chunks unindexed\n",
                         NumericalQuantityToString(sizes_for_index.size())));
          continue;
        }

        uint64_t total_index_bytes = std::accumulate(
            sizes_for_index.begin(), sizes_for_index.end(), 0ul);
        StrAppend(&out,
                  Substitute("\t\t$0: $1 chunks, $2 $3\n",
                             IndexTypeToString(index_type),
                             NumericalQuantityToString(sizes_for_index.size()),
                             BytesToString(total_index_bytes),
                             DistBytes(sizes_for_index)));
      }
    }

    return out;
  }

 private:
  static constexpr uint32_t kChunkSize = 1 << 12;
  using ChunkPtr = std::unique_ptr<ChunkStorageType>;
  using I = typename ChunkStorageType::IType;

  template <typename ConsumerF>
  void ConsumeRangesFromLatest(T from, T to, ConsumerF consumer) {
    auto it = latest_index_.lower_bound(from);
    while (it != latest_index_.end()) {
      if (it->first > to) {
        break;
      }

      for (I index : it->second) {
        consumer(Range<size_t>(index + kChunkSize * chunks_.size(), 1));
      }
      ++it;
    }
  }

  std::vector<T> latest_;
  std::map<T, std::vector<I>> latest_index_;

  // The chunks.
  std::vector<ChunkPtr> chunks_;
};

using IntegerStorage = Storage<int64_t, IntegerStorageChunk<uint16_t>>;
using BoolStorage = Storage<bool, BoolStorageChunk<uint16_t>>;

// Implementations

// -------------------- RangeSet --------------------

template <typename I>
RangeSet<I>::RangeSet(const std::vector<Range<I>>& ranges,
                      bool already_sorted) {
  if (ranges.empty()) {
    return;
  }

  ranges_ = ranges;
  if (!already_sorted) {
    std::sort(ranges_.begin(), ranges_.end());
  }
  RemoveOverlap(&ranges_);
}

template <typename I>
RangeSet<I>::RangeSet(std::vector<Range<I>>* ranges, bool already_sorted) {
  CHECK(ranges != nullptr);
  if (ranges->empty()) {
    return;
  }

  std::swap(ranges_, *ranges);
  if (!already_sorted) {
    std::sort(ranges_.begin(), ranges_.end());
  }
  RemoveOverlap(&ranges_);
}

template <typename I>
void RangeSet<I>::RemoveOverlap(std::vector<Range<I>>* ranges) {
  // Will first remove overlap.
  for (size_t i = 0; i < ranges->size() - 1; ++i) {
    const Range<I>& next_range = (*ranges)[i + 1];
    Range<I>& range = (*ranges)[i];

    I next_start_index = next_range.first;
    I start_index = range.first;
    I& count = range.second;
    I end_index = start_index + count;

    if (next_start_index < end_index) {
      I delta = end_index - next_start_index;
      CHECK(delta <= count);
      count -= delta;
    }
  }

  // And then merge consecutive ranges.
  std::vector<Range<I>> out;
  for (size_t i = 0; i < ranges->size(); ++i) {
    const Range<I>& ri = (*ranges)[i];
    I stretch_start = ri.first;
    I stretch_end = stretch_start;

    I stretch_end_i;
    for (stretch_end_i = i; stretch_end_i < ranges->size(); ++stretch_end_i) {
      const Range<I>& r_next = (*ranges)[stretch_end_i];

      I next_index = r_next.first;
      if (next_index < stretch_end) {
        LOG(FATAL) << "Ranges overlap, next index " << next_index
                   << " stretch end " << stretch_end;
      } else if (next_index == stretch_end) {
        stretch_end += r_next.second;
      } else {
        break;
      }
    }

    I stretch_len = stretch_end - stretch_start;
    if (stretch_len) {
      out.push_back({stretch_start, stretch_len});
    }

    i = stretch_end_i - 1;
  }

  std::swap(*ranges, out);
}

template <typename I>
void RangeSet<I>::Offset(I offset) {
  for (Range<>& range : ranges_) {
    range.first += offset;
  }
}

template <typename I>
uint64_t RangeSet<I>::ElementCount() const {
  uint64_t out = 0;
  for (const Range<>& range : ranges_) {
    out += range.second;
  }
  return out;
}

template <typename I>
bool RangeSet<I>::ContainsIndex(I index) const {
  auto it = std::lower_bound(ranges_.begin(), ranges_.end(), index,
                             [](const Range<>& lhs, uint64_t rhs) {
                               return (lhs.first + lhs.second - 1) < rhs;
                             });
  if (it == ranges_.end()) {
    return false;
  }

  return it->first <= index;
}

template <typename I>
std::string RangeSet<I>::ToString() const {
  std::vector<std::string> values;
  for (const Range<I>& range : ranges_) {
    std::string range_str =
        StrCat("[", std::to_string(range.first), ", ",
               std::to_string(range.first + range.second - 1), "]");
    values.emplace_back(range_str);
  }

  return StrCat("[", Join(values, ","), "]");
}

// -------------------- SortedSubsequence --------------------

template <typename I>
template <typename Container>
std::vector<SortedSubsequence<I>> SortedSubsequence<I>::Get(
    const Container& container) {
  if (container.size() == 0) {
    return {};
  }

  Range<I> current_range = {0, 1};
  bool is_increasing = true;
  bool is_increasing_set = false;

  std::vector<SortedSubsequence<I>> subsequences;
  for (size_t i = 0; i < container.size() - 1; ++i) {
    typename Container::value_type this_value = container.at(i);
    typename Container::value_type next_value = container.at(i + 1);

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

template <typename I>
template <typename Container>
I SortedSubsequence<I>::Bound(const Container& container,
                              typename Container::value_type value,
                              bool lower) const {
  I count = range_.second;
  I threshold = increasing_ ? range_.first : range_.first + range_.second - 1;

  I it;
  int64_t step;
  while (count > 0) {
    it = threshold;
    step = count / 2;
    if (increasing_) {
      it += step;
    } else {
      it -= step;
    }

    if (lower ? container.at(it) < value : !(value < container.at(it))) {
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

template <typename I>
template <typename Container>
Range<I> SortedSubsequence<I>::IndicesOfValuesInRange(
    const Container& container, typename Container::value_type from,
    typename Container::value_type to) const {
  CHECK(to >= from);
  I lower_from = Bound(container, from);
  I lower_to = Bound(container, to, false);

  if (increasing_) {
    return {lower_from, lower_to - lower_from};
  }
  return {lower_to + 1, lower_from - lower_to};
}

// -------------------- SortedIntervalIndex --------------------

template <typename Container, typename I>
template <typename ConsumerF>
void SortedIntervalIndex<Container, I>::ConsumeRanges(
    T from, T to, ConsumerF consumer) const {
  root_.Lookup(from, to, [this, from, to, consumer](const Interval& interval) {
    const SortedSubsequence<I>& subsequence = interval.value;
    Range<I> range = subsequence.IndicesOfValuesInRange(*container_, from, to);
    if (range.second) {
      if (!consumer(range)) {
        return false;
      }
    }

    return true;
  });
}

template <typename Container, typename I>
std::vector<typename SortedIntervalIndex<Container, I>::Interval>
SortedIntervalIndex<Container, I>::GetIntervals(
    const std::vector<SortedSubsequence<I>>& subsequences,
    const Container& container) {
  std::vector<Interval> out;
  for (const auto& subsequence : subsequences) {
    out.push_back({subsequence.MinValue(container),
                   subsequence.MaxValue(container), subsequence});
  }

  return out;
}

// -------------------- SortedIntervalIndex --------------------

template <typename I>
IntegerStorageChunk<I>::IntegerStorageChunk(const std::vector<int64_t>& values)
    : packed_int_vector_(make_unique<ImmutablePackedIntVector>(values)),
      rle_(make_unique<RLEField<int64_t>>(values)),
      indexed_(false) {
  if (packed_int_vector_->ByteEstimate() > rle_->ByteEstimate()) {
    packed_int_vector_.reset();
  } else {
    rle_.reset();
  }
}

template <typename I>
int64_t IntegerStorageChunk<I>::at(I index) const {
  if (packed_int_vector_) {
    return packed_int_vector_->at(index);
  }

  if (rle_) {
    return rle_->at(index);
  }

  LOG(FATAL) << "Storage not set";
  return 0;
}

template <typename I>
I IntegerStorageChunk<I>::size() const {
  if (packed_int_vector_) {
    return packed_int_vector_->size();
  }

  if (rle_) {
    return rle_->size();
  }

  LOG(FATAL) << "Storage not set";
  return 0;
}

template <typename I>
int64_t IntegerStorageChunk<I>::MinValue() const {
  if (packed_int_vector_) {
    return packed_int_vector_->min_value();
  }

  if (rle_) {
    return rle_->min_value();
  }

  LOG(FATAL) << "Storage not set";
  return 0;
}

template <typename I>
int64_t IntegerStorageChunk<I>::MaxValue() const {
  if (packed_int_vector_) {
    return packed_int_vector_->max_value();
  }

  if (rle_) {
    return rle_->max_value();
  }

  LOG(FATAL) << "Storage not set";
  return 0;
}

template <typename I>
uint64_t IntegerStorageChunk<I>::StorageByteEstimate() const {
  uint64_t total = 0;
  total += (packed_int_vector_ ? packed_int_vector_->ByteEstimate() : 0);
  total += (rle_ ? rle_->ByteEstimate() : 0);
  return total;
}

template <typename I>
uint64_t IntegerStorageChunk<I>::IndexByteEstimate() const {
  std::lock_guard<std::mutex> lock(index_mutex_);

  uint64_t total = 0;
  total += (basic_index_ ? basic_index_->ByteEstimate() : 0);
  total += (packed_index_ ? packed_index_->ByteEstimate() : 0);
  total += (rle_index_ ? rle_index_->ByteEstimate() : 0);
  return total;
}

template <typename I>
IndexType IntegerStorageChunk<I>::IndexType() const {
  std::lock_guard<std::mutex> lock(index_mutex_);

  if (basic_index_) {
    return IndexType::BASIC;
  }

  if (rle_index_ || packed_index_) {
    return IndexType::SORTED_INTERVAL;
  }

  return IndexType::UNINDEXED;
}

template <typename I>
StorageType IntegerStorageChunk<I>::StorageType() const {
  if (packed_int_vector_) {
    return StorageType::INT_PACKED;
  }

  if (rle_) {
    return StorageType::RLE;
  }

  LOG(FATAL) << "Storage not set";
  return StorageType::INT_PACKED;
}

template <typename I>
void IntegerStorageChunk<I>::Index() {
  std::lock_guard<std::mutex> lock(index_mutex_);

  if (indexed_) {
    return;
  }

  if (packed_int_vector_) {
    basic_index_ = make_unique<BasicIndex<int64_t, I>>(*packed_int_vector_);
    packed_index_ = make_unique<SortedIntervalIndex<ImmutablePackedIntVector, I>>(
        packed_int_vector_.get());

    if (basic_index_->ByteEstimate() > packed_index_->ByteEstimate()) {
      basic_index_.reset();
    } else {
      packed_index_.reset();
    }
  } else {
    basic_index_ = make_unique<BasicIndex<int64_t, I>>(*rle_);
    rle_index_ =
        make_unique<SortedIntervalIndex<RLEField<int64_t>>>(rle_.get());

    if (basic_index_->ByteEstimate() > rle_index_->ByteEstimate()) {
      basic_index_.reset();
    } else {
      rle_index_.reset();
    }
  }

  indexed_ = true;
}

template <typename I>
template <typename ConsumerF>
void IntegerStorageChunk<I>::ConsumeRanges(int64_t from, int64_t to, ConsumerF consumer) {
  CHECK(to >= from);
  int64_t min = MinValue();
  int64_t max = MaxValue();
  if (to < min || from > max) {
    return;
  }

  if (to >= max && from <= min) {
    consumer(Range<I>(0, size()));
    return;
  }

  Index();

  if (basic_index_) {
    basic_index_->ConsumeRanges(from, to, consumer);
    return;
  }

  if (packed_index_) {
    packed_index_->ConsumeRanges(from, to, consumer);
    return;
  }

  if (rle_index_) {
    rle_index_->ConsumeRanges(from, to, consumer);
    return;
  }

  LOG(FATAL) << "Index not set";
}

}  // namespace nc

#endif
