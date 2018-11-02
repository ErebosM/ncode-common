// A memory-efficient column of numbers.

#ifndef NCODE_INTERVAL_TREE
#define NCODE_INTERVAL_TREE

#include <set>
#include <vector>
#include "common.h"
#include "stats.h"

namespace nc {
namespace interval_tree {

// An interval of [low, high] numbers associated with a value. The value sohuld
// be copiable and comparable.
template <typename T, typename V>
struct Interval {
  T low;
  T high;
  V value;

  bool operator==(const Interval& other) const {
    return std::tie(low, high, value) ==
           std::tie(other.low, other.high, other.value);
  }
};

// An interval along with a sequential index. Used internally by interval trees
// to perform quick set operations.
template <typename T, typename V>
using IndexedInterval = std::pair<Interval<T, V>, uint32_t>;

// Returns the number of intervals that contain a point.
template <typename T, typename V>
uint64_t ContainingIntervalCount(
    const std::vector<IndexedInterval<T, V>>& intervals, T point) {
  uint64_t count = 0;
  for (const auto& interval : intervals) {
    T min = interval.first.low;
    T max = interval.first.high;

    if (point >= min && point <= max) {
      ++count;
    }
  }
  return count;
}

// Attempts to pick a point that will be contained in the most intervals.
template <typename T, typename V>
T PickPointForSplitting(const std::vector<IndexedInterval<T, V>>& intervals) {
  // Will first try in 1% increments. If that does not work will attempt the
  // midpoints of all intervals.
  T min_all = std::numeric_limits<T>::max();
  T max_all = std::numeric_limits<T>::min();
  for (const auto& interval : intervals) {
    CHECK(interval.first.low <= interval.first.high);
    min_all = std::min(min_all, interval.first.low);
    max_all = std::max(max_all, interval.first.high);
  }

  uint64_t best_interval_count = 0;
  T best_point;

  T delta = max_all - min_all;
  for (size_t i = 0; i < 100; ++i) {
    T test_point = min_all + delta * (i / 99.0);
    uint64_t count = ContainingIntervalCount(intervals, test_point);
    if (count > best_interval_count) {
      best_interval_count = count;
      best_point = test_point;
    }
  }

  if (best_interval_count == 0) {
    for (const auto& interval : intervals) {
      T min_interval = interval.first.low;
      T max_interval = interval.first.high;
      T test_point = min_interval + (max_interval - min_interval) / 2;
      uint64_t count = ContainingIntervalCount(intervals, test_point);
      if (count > best_interval_count) {
        best_interval_count = count;
        best_point = test_point;
      }
    }
  }

  CHECK(best_interval_count != 0);
  return best_point;
}

template <typename T, typename V>
std::tuple<std::vector<IndexedInterval<T, V>>,
           std::vector<IndexedInterval<T, V>>,
           std::vector<IndexedInterval<T, V>>, T>
Split(const std::vector<IndexedInterval<T, V>>& intervals) {
  T point = PickPointForSplitting(intervals);
  std::vector<IndexedInterval<T, V>> less, more, intersecting;
  for (const auto& interval : intervals) {
    T min = interval.first.low;
    T max = interval.first.high;

    if (max < point) {
      less.emplace_back(interval);
    } else if (min > point) {
      more.emplace_back(interval);
    } else {
      intersecting.emplace_back(interval);
    }
  }

  return {less, more, intersecting, point};
}

template <typename T, typename V>
class Node {
 public:
  Node(const std::vector<IndexedInterval<T, V>>& intervals, T mid_value,
       std::unique_ptr<Node> less, std::unique_ptr<Node> more)
      : mid_value_(mid_value), less_(std::move(less)), more_(std::move(more)) {
    CHECK(!intervals.empty());
    min_sorted_ = intervals;
    std::sort(
        min_sorted_.begin(), min_sorted_.end(),
        [](const IndexedInterval<T, V>& lhs, const IndexedInterval<T, V>& rhs) {
          return lhs.first.low < rhs.first.low;
        });

    max_sorted_.reserve(min_sorted_.size());
    for (const auto& interval : min_sorted_) {
      max_sorted_.emplace_back(&interval);
    }
    std::sort(
        max_sorted_.begin(), max_sorted_.end(),
        [](const IndexedInterval<T, V>* lhs, const IndexedInterval<T, V>* rhs) {
          return lhs->first.high > rhs->first.high;
        });
  }

  // Produces all intervals that intersect with a point. The signature of
  // LookupF should be bool(const Interval<T, V>&).
  template <typename LookupF>
  void Lookup(T point, LookupF consumer) const {
    if (point < mid_value_) {
      // All ranges end past mid_value_.
      for (const IndexedInterval<T, V>& interval : min_sorted_) {
        if (interval.first.low > point) {
          break;
        }

        if (!consumer(interval.first, interval.second)) {
          return;
        }
      }

      if (less_) {
        less_->Lookup(point, consumer);
      }
    } else if (point > mid_value_) {
      // All ranges start before mid_value_.
      for (const IndexedInterval<T, V>* interval : max_sorted_) {
        if (interval->first.high < point) {
          break;
        }

        if (!consumer(interval->first, interval->second)) {
          return;
        }
      }

      if (more_) {
        more_->Lookup(point, consumer);
      }
    } else {
      for (const IndexedInterval<T, V>& interval : min_sorted_) {
        if (!consumer(interval.first, interval.second)) {
          return;
        }
      }
    }
  }

  void Walk(std::function<void(const IndexedInterval<T, V>&)> consumer) const {
    for (const auto& interval : min_sorted_) {
      consumer(interval);
    }
    if (less_) {
      less_->Walk(consumer);
    }
    if (more_) {
      more_->Walk(consumer);
    }
  }

  uint64_t ByteEstimate() const {
    uint64_t less_size = less_ ? less_->ByteEstimate() : 0;
    uint64_t more_size = more_ ? more_->ByteEstimate() : 0;
    return sizeof(this) + sizeof(void*) * max_sorted_.capacity() +
           sizeof(IndexedInterval<T, V>) * min_sorted_.capacity() + less_size +
           more_size;
  }

 private:
  // All intervals have a min value less than or equal to this, and max value
  // more than or equal to this.
  T mid_value_;
  std::vector<IndexedInterval<T, V>> min_sorted_;
  std::vector<const IndexedInterval<T, V>*> max_sorted_;
  std::unique_ptr<Node<T, V>> less_;
  std::unique_ptr<Node<T, V>> more_;
};

template <typename T, typename V>
class TreeRoot {
 public:
  TreeRoot(const std::vector<Interval<T, V>>& intervals)
      : root_(BuildTree(AssignIndices(intervals))) {
    if (!root_) {
      return;
    }

    root_->Walk([this](const IndexedInterval<T, V>& interval) {
      intervals_sorted_.emplace_back(interval.first.low, &interval);
      intervals_sorted_.emplace_back(interval.first.high, &interval);
    });

    std::sort(intervals_sorted_.begin(), intervals_sorted_.end(),
              [](const std::pair<T, const IndexedInterval<T, V>*>& lhs,
                 const std::pair<T, const IndexedInterval<T, V>*>& rhs) {
                return lhs.first < rhs.first;
              });
  }

  // Looks up intervals that contain single point. The signature of LookupF
  // should be bool(const Interval<T, V>&, uint32_t).
  template <typename LookupF>
  void Lookup(T point, LookupF consumer) const {
    if (!root_) {
      return;
    }

    root_->Lookup(point, consumer);
  }

  // Looks up intervals that have at least one value contained in a range. The
  // signature of LookupF should be bool(const Interval<T, V>&, uint32_t).
  template <typename LookupF>
  void Lookup(T min_point, T max_point, LookupF consumer) const {
    if (!root_) {
      return;
    }

    size_t num_intervals = intervals_sorted_.size() / 2;
    std::vector<bool> already_consumed(num_intervals, false);

    auto it = std::lower_bound(
        intervals_sorted_.begin(), intervals_sorted_.end(), min_point,
        [](const std::pair<T, const IndexedInterval<T, V>*>& lhs, T rhs) {
          return lhs.first < rhs;
        });
    while (it != intervals_sorted_.end()) {
      if (it->first > max_point) {
        break;
      }

      const IndexedInterval<T, V>* interval = it->second;
      uint32_t interval_index = interval->second;
      if (!already_consumed[interval_index]) {
        if (!consumer(interval->first)) {
          return;
        }
        already_consumed[interval_index] = true;
      }

      ++it;
    }

    T point = min_point + (max_point - min_point) / 2;
    CHECK(point >= min_point);

    Lookup(point, [&already_consumed, &consumer](const Interval<T, V>& interval,
                                                 uint32_t interval_index) {
      if (!already_consumed[interval_index]) {
        if (!consumer(interval)) {
          return false;
        }
        already_consumed[interval_index] = true;
      }

      return true;
    });
  }

  uint64_t ByteEstimate() const {
    return root_->ByteEstimate() +
           sizeof(std::pair<T, void*>) * intervals_sorted_.capacity() +
           sizeof(this);
  }

 private:
  static std::vector<IndexedInterval<T, V>> AssignIndices(
      const std::vector<Interval<T, V>>& intervals) {
    std::vector<IndexedInterval<T, V>> out;
    for (size_t i = 0; i < intervals.size(); ++i) {
      out.emplace_back(intervals[i], i);
    }
    return out;
  }

  static std::unique_ptr<Node<T, V>> BuildTree(
      const std::vector<IndexedInterval<T, V>>& intervals) {
    if (intervals.empty()) {
      return {};
    }

    std::vector<IndexedInterval<T, V>> less, more, intersecting;
    T point;
    std::tie(less, more, intersecting, point) = Split(intervals);

    return make_unique<Node<T, V>>(intersecting, point, BuildTree(less),
                                   BuildTree(more));
  }

  std::unique_ptr<Node<T, V>> root_;

  // Intervals sorted by their lower and upper limits.
  std::vector<std::pair<T, const IndexedInterval<T, V>*>> intervals_sorted_;
};

}  // namespace interval_tree
}  // namespace nc
#endif
