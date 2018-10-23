#ifndef NCODE_COMMON_H
#define NCODE_COMMON_H

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdbool>
#include <cstdint>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

#include "logging.h"
#include "map_util.h"

namespace nc {

// Used to silence unused parameter warnings from the compiler.
template <typename T>
void Unused(T&&) {}

// make_unique "lifted" from C++14.
template <class T>
struct _Unique_if {
  typedef std::unique_ptr<T> _Single_object;
};

template <class T>
struct _Unique_if<T[]> {
  typedef std::unique_ptr<T[]> _Unknown_bound;
};

template <class T, size_t N>
struct _Unique_if<T[N]> {
  typedef void _Known_bound;
};

template <class T, class... Args>
typename _Unique_if<T>::_Single_object make_unique(Args&&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

template <class T>
typename _Unique_if<T>::_Unknown_bound make_unique(size_t n) {
  typedef typename std::remove_extent<T>::type U;
  return std::unique_ptr<T>(new U[n]());
}

template <class T, class... Args>
typename _Unique_if<T>::_Known_bound make_unique(Args&&...) = delete;

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

// Lifted from http://www.exploringbinary.com
constexpr int IsPowerOfTwo(uint32_t x) {
  return ((x != 0) && ((x & (~x + 1)) == x));
}

// Returns |old - new| / old
template <typename T>
constexpr double FractionalDifference(T old_value, T new_value) {
  return std::abs(old_value - new_value) / old_value;
}

// Returns true if all arguments are equal.
template <typename T, typename U>
constexpr bool AllEqual(const T& t, const U& u) {
  return t == u;
}

template <typename T, typename U, typename... Others>
constexpr bool AllEqual(const T& t, const U& u, Others const&... args) {
  return (t == u) && AllEqual(u, args...);
}

// A typesafe wrapper around an unsigned integer type.
template <typename Tag, typename T, char ZeroRepr = '\0'>
class TypesafeUintWrapper {
 public:
  static_assert(std::is_integral<T>::value, "Integral type needed");
  static_assert(std::is_unsigned<T>::value, "Unsigned type needed");

  static constexpr TypesafeUintWrapper Zero() { return TypesafeUintWrapper(0); }
  static constexpr TypesafeUintWrapper Max() {
    return TypesafeUintWrapper(std::numeric_limits<T>::max());
  }

  // Explicit constructor.
  explicit constexpr TypesafeUintWrapper(T val) : m_val_(val) {}

  constexpr T Raw() const { return m_val_; }
  inline bool IsZero() const { return m_val_ == 0; }
  inline bool IsNotZero() const { return m_val_ != 0; }

  friend bool operator==(TypesafeUintWrapper a, TypesafeUintWrapper b) {
    return a.m_val_ == b.m_val_;
  }

  friend bool operator!=(TypesafeUintWrapper a, TypesafeUintWrapper b) {
    return a.m_val_ != b.m_val_;
  }

  friend bool operator<(TypesafeUintWrapper a, TypesafeUintWrapper b) {
    return a.m_val_ < b.m_val_;
  }

  friend bool operator>(TypesafeUintWrapper a, TypesafeUintWrapper b) {
    return a.m_val_ > b.m_val_;
  }

  friend bool operator<=(TypesafeUintWrapper a, TypesafeUintWrapper b) {
    return a.m_val_ <= b.m_val_;
  }

  friend bool operator>=(TypesafeUintWrapper a, TypesafeUintWrapper b) {
    return a.m_val_ >= b.m_val_;
  }

  friend std::ostream& operator<<(std::ostream& output,
                                  const TypesafeUintWrapper& op) {
    if (op.m_val_ == 0 && ZeroRepr != '\0') {
      output << ZeroRepr;
    } else {
      output << std::to_string(op.m_val_);
    }
    return output;
  }

  T* RawPtr() { return &m_val_; }

 protected:
  T m_val_;
};

// A class for typesafe integer indices. Similar to TypesafeUintWrapper, but can
// be auto-cast to size_t, which allows indexing.
template <class Tag, class V = uint32_t>
class Index {
 public:
  static_assert(std::is_integral<V>::value, "Integral type needed");
  static_assert(std::is_unsigned<V>::value, "Unsigned type needed");

  explicit Index() : m_val_(0) {}
  explicit Index(V val) : m_val_(val) {}
  operator size_t() const { return m_val_; }

 private:
  V m_val_;
};

template <typename Iter, typename RandomGenerator>
Iter SelectRandomly(Iter start, Iter end, RandomGenerator* g) {
  std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
  std::advance(start, dis(*g));
  return start;
}

// Generates a random uint64_t identifier that has not been taken yet. Will try
// to generate small values first. Will use the provided random number
// generator, or use a new one with seed of 1 if no random number generator is
// provided.
uint64_t GetRandomId(std::function<bool(uint64_t)> id_taken,
                     std::mt19937* rnd = nullptr);

uint32_t ghtonl(uint32_t x);

// A priority queue that uses a vector for storage and exposes a const reference
// to it.
template <class T,
          class Compare = std::less<typename std::vector<T>::value_type>>
class VectorPriorityQueue {
 public:
  // True is size() == 0.
  bool empty() const { return container_.empty(); }

  // Number of elements in queue.
  size_t size() const { return container_.size(); }

  // Const reference to the top element.
  const T& top() const { return container_.front(); }

  // Returns the top element and pops it.
  T PopTop() {
    T tmp = std::move(container_.front());
    pop();
    return std::move(tmp);
  }

  // Removes the top element from the queue.
  void pop() {
    std::pop_heap(container_.begin(), container_.end(), compare_);
    container_.pop_back();
  }

  // Pushes a new element to the queue.
  template <class... Args>
  void emplace(Args&&... args) {
    container_.emplace_back(std::forward<Args>(args)...);
    std::push_heap(container_.begin(), container_.end(), compare_);
  }

  // Const reference to the underlying vector. The vector will change as
  // elements are pushed / popped, so do not store any references/pointers to
  // its elements.
  const std::vector<T>& containter() const { return container_; }

 private:
  std::vector<T> container_;
  Compare compare_;
};

// Glob-expands a pattern to a list of strings.
std::vector<std::string> Glob(const std::string& pat);

// Defines how a ThresholdEnforcer works.
class ThresholdEnforcerPolicy {
 public:
  ThresholdEnforcerPolicy()
      : empty_threshold_absolute_(0),
        threshold_absolute_(0),
        threshold_relative_to_total_(0),
        threshold_relative_to_current_(0) {}

  // The empty threshold is the minimum value that is required to be associated
  // with a key. Any change that will cause a key to have value of less than
  // this threshold is disallowed. The empty threshold is always an absolute
  // number.
  void set_empty_threshold_absolute(double empty_threshold_absolute);
  double empty_threshold_absolute() const;

  // If a change for a key is less than this value (absolute) it is
  // ignored.
  void set_threshold_absolute(double threshold_absolute);
  double threshold_absolute() const;

  // Same as set_threshold_absolute, but relative the current value associated
  // with a key
  void set_threshold_relative_to_current(double threshold_relative_to_current);
  double threshold_relative_to_current() const;

  // Same as set_threshold_absolute, but relative to the total of all values.
  void set_threshold_relative_to_total(double threshold_relative_to_total);
  double threshold_relative_to_total() const;

 private:
  double empty_threshold_absolute_;
  double threshold_absolute_;
  double threshold_relative_to_total_;
  double threshold_relative_to_current_;
};

// Given a series of changes to numeric values indexed by a key of type T this
// class will either allow or disallow a subsequent change according to a
// thresholding policy.
template <typename T>
class ThresholdEnforcer {
 public:
  explicit ThresholdEnforcer(const ThresholdEnforcerPolicy& enforcer_policy,
                             double missing_value = 0)
      : policy_(enforcer_policy), missing_value_(missing_value) {}

  // Changes the value associated with a given key. Will return false if the
  // change is not allowed, will return true and make the change if the change
  // is allowed.
  bool Change(const T& key, double value) {
    if (!CanChange(value, Get(key))) {
      return false;
    }

    current_state_[key] = value;
    return true;
  }

  // Returns true if any of the values in the given map can be changed. If no
  // values pass the threshold for change returns false. If true is returned the
  // current values will become those of the new state. If false is returned the
  // current values are not changed. Keys that are missing in the new state, but
  // are in the current one and keys that are missing in the current state, but
  // are in the new one will be assumed to have the value 'missing_value_'.
  bool ChangeBulk(const std::map<T, double>& new_state) {
    for (const auto& key_and_new_value : new_state) {
      const T& key = key_and_new_value.first;
      double value = key_and_new_value.second;
      if (CanChange(value, Get(key))) {
        current_state_ = new_state;
        return true;
      }
    }

    // No keys from the new state can be changed, but what about keys in the
    // current state that are not in the new state.
    for (const auto& key_and_current_value : current_state_) {
      const T& key = key_and_current_value.first;
      double current_value = key_and_current_value.second;
      if (!ContainsKey(new_state, key)) {
        if (CanChange(missing_value_, current_value)) {
          current_state_ = new_state;
          return true;
        }
      }
    }

    return false;
  }

  // Returns the value associated with a key or missing_value.
  double Get(const T& key) const {
    const double* value_ptr = FindOrNull(current_state_, key);
    if (value_ptr == nullptr) {
      return missing_value_;
    }
    return *value_ptr;
  }

 private:
  bool CanChange(double value, double current_value) {
    double delta_abs = std::abs(current_value - value);
    if (delta_abs < policy_.threshold_absolute()) {
      return false;
    }

    double delta_abs_from_missing = std::abs(missing_value_ - value);
    if (delta_abs_from_missing < policy_.empty_threshold_absolute()) {
      return false;
    }

    double delta_relative_to_current;
    if (current_value > 0.0) {
      delta_relative_to_current =
          std::abs((value - current_value) / current_value);
    } else {
      delta_relative_to_current = 1.0;
    }

    if (delta_relative_to_current < policy_.threshold_relative_to_current()) {
      return false;
    }

    double total = std::accumulate(
        current_state_.begin(), current_state_.end(), 0.0,
        [](double total, const std::pair<T, double>& key_and_value) {
          return total + key_and_value.second;
        });

    double delta_relative_to_total;
    if (total > 0.0) {
      delta_relative_to_total = std::abs(value / total);
    } else {
      delta_relative_to_total = 1.0;
    }

    if (delta_relative_to_total < policy_.threshold_relative_to_total()) {
      return false;
    }

    return true;
  }

  const ThresholdEnforcerPolicy policy_;

  // Missing keys are considered to have this value.
  double missing_value_;

  // The current key->value relationship.
  std::map<T, double> current_state_;
};

// Policy that specifies how to time keys out.
class TimeoutPolicy {
 public:
  TimeoutPolicy()
      : base_timeout_(0),
        timeout_penalty_(0),
        timeout_penalty_lookback_(0),
        timeout_penalty_cumulative_(false) {}

  // Base amount of time before a key becomes eligible for timeout.
  uint64_t base_timeout() const { return base_timeout_; }
  void set_base_timeout(uint64_t base_timeout) { base_timeout_ = base_timeout; }

  // A penalty of 'timeout_penalty' will be added on top of base_timeout to keys
  // that have been updated in the last 'timeout_penalty_lookback'.
  uint64_t timeout_penalty() const { return timeout_penalty_; }
  void set_timeout_penalty(uint64_t timeout_penalty);
  uint64_t timeout_penalty_lookback() const;
  void set_timeout_penalty_lookback(uint64_t timeout_penalty_lookback);

  // The timeout penalty can be cumulative -- applied each time an update for
  // the key is seen over the lookback period, or one-off.
  bool timeout_penalty_cumulative() const;
  void set_timeout_penalty_cumulative(bool timeout_penalty_cumulative);

 private:
  uint64_t base_timeout_;
  uint64_t timeout_penalty_;
  uint64_t timeout_penalty_lookback_;
  bool timeout_penalty_cumulative_;
};

// A class that can be used to time out keys, based on a timeout policy. Time is
// defined as a simple uint64_t value, the units in the timeout policy should
// match those of the calls to methods of this class.
template <typename T>
class TimeoutEnforcer {
 public:
  TimeoutEnforcer(const TimeoutPolicy& timeout_policy)
      : timeout_policy_(timeout_policy) {}

  // Updates a given key at a given point in time. Either adds a new key or
  // "freshens" an old key and causes it to time out later.
  void Update(T key, uint64_t now) {
    current_keys_[key] = now;

    std::vector<uint64_t>& history = key_to_history_[key];
    if (!history.empty()) {
      CHECK(history.back() <= now) << "Decreasing time";
    }

    history.emplace_back(now);
  }

  // Times out a number of keys at a given point in time. This function will
  // remove the keys -- i.e. for any returned key any repeated call to Timeout
  // before Update will not return the same key.
  std::vector<T> Timeout(uint64_t now) {
    std::vector<T> eligible;
    for (auto it = current_keys_.cbegin(); it != current_keys_.cend();) {
      const T& key = it->first;
      uint64_t update_time = it->second;

      // Will look through the history to figure out how many times this key was
      // updated over the penalty lookback.
      size_t times_updated = 0;
      const std::vector<uint64_t>& history = key_to_history_[key];
      for (auto rit = history.rbegin(); rit != history.rend(); ++rit) {
        uint64_t update_time = *rit;
        uint64_t threshold =
            std::max(now, timeout_policy_.timeout_penalty_lookback()) -
            std::min(now, timeout_policy_.timeout_penalty_lookback());
        if (update_time <= threshold) {
          break;
        }

        ++times_updated;
      }

      if (times_updated > 0 && !timeout_policy_.timeout_penalty_cumulative()) {
        times_updated = 1;
      }
      uint64_t penalty = timeout_policy_.timeout_penalty() * times_updated;

      CHECK(now >= update_time) << "Decreasing time";
      uint64_t delta = now - update_time;
      if (delta >= (timeout_policy_.base_timeout() + penalty)) {
        eligible.emplace_back(key);
        it = current_keys_.erase(it);
      } else {
        ++it;
      }
    }

    return eligible;
  }

  bool IsInCurrentKeys(const T& key) const {
    return ContainsKey(current_keys_, key);
  }

  std::vector<T> AllCurrentKeys() const {
    std::vector<T> keys;
    keys.reserve(current_keys_.size());
    for (const auto& key_and_update_time : current_keys_) {
      keys.emplace_back(key_and_update_time.first);
    }
    return keys;
  }

  void Clear() {
    current_keys_.clear();
    key_to_history_.clear();
  }

 private:
  const TimeoutPolicy timeout_policy_;

  // Keys, along with when they were updated.
  std::map<T, uint64_t> current_keys_;

  // History, used when applying penalties.
  std::map<T, std::vector<uint64_t>> key_to_history_;
};

// A class that is given a (real) time budget upon construction and can later be
// queried if the budget has expired.
class CountdownTimer {
 public:
  CountdownTimer(std::chrono::nanoseconds budget);

  // Returns true if more time has elapsed between the time this object was
  // created and the current time than the budget.
  bool Expired() const;

  // The remaining time.
  std::chrono::nanoseconds RemainingTime() const;

 private:
  std::chrono::steady_clock::time_point construction_time_;
  std::chrono::nanoseconds budget_;
};

// Records the time after creation. Optionally can add elapsed time until
// destruction to a value.
class Timer {
 public:
  Timer(std::chrono::nanoseconds* total_duration = nullptr);

  ~Timer();

  std::chrono::nanoseconds TimeSoFarNanos() const;

  std::chrono::microseconds TimeSoFarMicros() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        TimeSoFarNanos());
  }

  std::chrono::milliseconds TimeSoFarMillis() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        TimeSoFarNanos());
  }

  void Reset();

 private:
  std::chrono::nanoseconds start_time_;
  std::chrono::nanoseconds* total_duration_;
};

// Generates a random string of a given length. The string will contain
// A-Za-z0-9.
std::string RandomString(size_t length);

// Prints a double with given precision.
std::string ToStringMaxDecimals(double value, int decimals);

// Returns true if a sequence of values is sorted.
template<typename T>
bool IsSorted(const std::vector<T>& values, bool reverse) {
  CHECK(values.size() > 0);
  for (size_t i = 0; i < values.size() - 1; ++i) {
    if (reverse ? values[i] < values[i + 1] : values[i] > values[i + 1]) {
      return false;
    }
  }

  return true;
}

// Returns true if a sequence has at least one negative value.
template<typename T>
bool HasNegativeValues(const std::vector<T>& values) {
  for (const T& v : values) {
    if (v < 0) {
      return true;
    }
  }

  return false;
}

}  // namespace nc

#endif /* HT2SIM_COMMON_H */
