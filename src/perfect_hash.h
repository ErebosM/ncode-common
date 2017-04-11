#ifndef NCODE_PERFECT_HASH_H
#define NCODE_PERFECT_HASH_H

#include <limits>
#include <type_traits>
#include <vector>
#include <set>

#include "common.h"

namespace nc {

// Assigns a unique incremental ID to items. This ID can later be used in
// perfect hashing sets and maps. The set of items cannot shrink -- once an item
// is added it cannot be removed. The first argument is the type of the items,
// the second argument is the type of the index. It should be an unsigned
// integer type. If more items are added than can be indexed with V will
// CHECK-fail. The third argument is a tag that will uniquely identify the
// items. This way two sets with the same item type will not have the same type.
template <typename T, typename V, typename Tag>
class PerfectHashStore {
 public:
  using IndexType = Index<Tag, V>;

  // Adds a copy of a new item to the set of items.
  IndexType AddItem(T item) {
    CHECK(items_.size() < std::numeric_limits<V>::max());
    IndexType next_index(items_.size());
    items_.emplace_back(item);
    return next_index;
  }

  // Adds and moves a new item to the set of items.
  IndexType MoveItem(T&& item) {
    CHECK(items_.size() < std::numeric_limits<V>::max());
    IndexType next_index(items_.size());
    items_.emplace_back(std::move(item));
    return next_index;
  }

  // Returns the address of the item that corresponds to an index. The address
  // may not be valid after additional calls to AddItem/MoveItem.
  const T* GetItemOrNull(IndexType index) const {
    if (index >= items_.size()) {
      return nullptr;
    }

    return &items_[index];
  }

  const T& GetItemOrDie(IndexType index) const {
    CHECK(index < items_.size());
    return items_[index];
  }

  size_t size() const { return items_.size(); }

 private:
  std::vector<T> items_;
};

// A set that contains indices with O(1) operations.
template <typename V, typename Tag>
class PerfectHashSet {
 public:
  class ConstIterator {
   public:
    using value_type = Index<Tag, V>;

    ConstIterator(const PerfectHashSet<V, Tag>* parent, Index<Tag, V> index)
        : parent_(parent), index_(index) {}

    ConstIterator operator++() {
      while (index_ != parent_->set_.size()) {
        index_ = Index<Tag, V>(index_ + 1);
        if (parent_->Contains(index_)) {
          return *this;
        }
      }
      return *this;
    }

    bool operator!=(const ConstIterator& other) {
      return index_ != other.index_;
    }

    Index<Tag, V> operator*() const { return index_; }

   private:
    const PerfectHashSet<V, Tag>* parent_;
    Index<Tag, V> index_;
  };

  // Returns a set with all items in the store.
  template <typename T>
  static PerfectHashSet<V, Tag> FullSetFromStore(
      const PerfectHashStore<T, V, Tag>& store) {
    PerfectHashSet<V, Tag> out;
    out.set_.resize(store.size(), true);
    return out;
  }

  PerfectHashSet() {}

  // Builds a set with the elements in the initializer list.
  PerfectHashSet(std::initializer_list<Index<Tag, V>> init_list) {
    for (Index<Tag, V> i : init_list) {
      Insert(i);
    }
  }

  explicit PerfectHashSet(const std::set<Index<Tag, V>>& init_set) {
    for (Index<Tag, V> i : init_set) {
      Insert(i);
    }
  }

  // Adds all elements from another set to this one.
  void InsertAll(const PerfectHashSet<V, Tag>& other) {
    size_t other_size = other.set_.size();
    set_.resize(std::max(set_.size(), other_size), false);
    for (size_t i = 0; i < other_size; ++i) {
      if (other.set_[i]) {
        set_[i] = true;
      }
    }
  }

  // Removes all elements from another set to this one.
  void RemoveAll(const PerfectHashSet<V, Tag>& other) {
    size_t min_size = std::min(set_.size(), other.set_.size());
    for (size_t i = 0; i < min_size; ++i) {
      if (other.set_[i]) {
        set_[i] = false;
      }
    }
  }

  void Insert(Index<Tag, V> index) {
    set_.resize(std::max(set_.size(), index + 1), false);
    set_[index] = true;
  }

  void insert(Index<Tag, V> index) { Insert(index); }

  void Remove(Index<Tag, V> index) {
    if (set_.size() > index) {
      set_[index] = false;
    }
  }

  bool Empty() const {
    for (char x : set_) {
      if (x) {
        return false;
      }
    }

    return true;
  }

  void Clear() { set_.clear(); }

  bool Contains(Index<Tag, V> index) const {
    if (set_.size() > index) {
      return set_[index];
    }

    return false;
  }

  // Returns true if this set shares any elements in common with another one.
  bool Intersects(const PerfectHashSet<V, Tag>& other) const {
    const std::vector<char>& other_set = other.set_;
    size_t to = std::min(other_set.size(), set_.size());
    for (size_t i = 0; i < to; ++i) {
      if (set_[i] && other_set[i]) {
        return true;
      }
    }

    return false;
  }

  // Given this set and another will return a set with the elements from
  // 'other' that are not in this set.
  PerfectHashSet<V, Tag> Difference(const PerfectHashSet<V, Tag>& other) const {
    const std::vector<char>& other_set = other.set_;
    PerfectHashSet<V, Tag> out;

    size_t i;
    for (i = 0; i < other_set.size(); ++i) {
      if (other_set[i]) {
        if (i >= set_.size() || !set_[i]) {
          out.Insert(Index<Tag, V>(i));
        }
      }
    }

    return out;
  }

  // Same as calling Difference and checking if the returned set is empty.
  // Returns false if there is at least one element in 'other' that is not in
  // this set.
  bool DifferenceEmpty(const PerfectHashSet<V, Tag>& other) const {
    const std::vector<char>& other_set = other.set_;

    size_t i;
    for (i = 0; i < other_set.size(); ++i) {
      if (other_set[i]) {
        if (i >= set_.size() || !set_[i]) {
          return false;
        }
      }
    }

    return true;
  }

  size_t Count() const { return std::count(set_.begin(), set_.end(), true); }

  ConstIterator begin() const {
    size_t i;
    for (i = 0; i < set_.size(); ++i) {
      if (set_[i]) {
        break;
      }
    }

    return {this, Index<Tag, V>(i)};
  }

  ConstIterator end() const { return {this, Index<Tag, V>(set_.size())}; }

  friend bool operator<(const PerfectHashSet<V, Tag>& a,
                        const PerfectHashSet<V, Tag>& b) {
    return a.set_ < b.set_;
  }

  friend bool operator==(const PerfectHashSet<V, Tag>& a,
                         const PerfectHashSet<V, Tag>& b) {
    return a.set_ == b.set_;
  }

 private:
  std::vector<char> set_;
};

// A map from index to a value with O(1) operations.
template <typename V, typename Tag, typename Value>
class PerfectHashMap {
 public:
  class Iterator {
   public:
    Iterator(PerfectHashMap<V, Tag, Value>* parent, Index<Tag, V> index)
        : parent_(parent), index_(index) {}
    Iterator operator++() {
      while (index_ != parent_->values_.size()) {
        index_ = Index<Tag, V>(index_ + 1);
        if (parent_->HasValue(index_)) {
          return *this;
        }
      }
      return *this;
    }
    bool operator!=(const Iterator& other) { return index_ != other.index_; }
    std::pair<Index<Tag, V>, Value*> operator*() const {
      return std::make_pair(index_, &parent_->values_[index_].second);
    }

   private:
    PerfectHashMap<V, Tag, Value>* parent_;
    Index<Tag, V> index_;
  };

  class ConstIterator {
   public:
    ConstIterator(const PerfectHashMap<V, Tag, Value>* parent,
                  Index<Tag, V> index)
        : parent_(parent), index_(index) {}
    ConstIterator operator++() {
      while (index_ != parent_->values_.size()) {
        index_ = Index<Tag, V>(index_ + 1);
        if (parent_->HasValue(index_)) {
          return *this;
        }
      }
      return *this;
    }
    bool operator!=(const ConstIterator& other) {
      return index_ != other.index_;
    }
    std::pair<Index<Tag, V>, const Value*> operator*() const {
      return std::make_pair(index_, &parent_->values_[index_].second);
    }

   private:
    const PerfectHashMap<V, Tag, Value>* parent_;
    Index<Tag, V> index_;
  };

  // Adds a new value.
  void Add(Index<Tag, V> index, Value value) {
    values_.resize(std::max(values_.size(), index + 1));
    values_[index] = {true, value};
  }

  // Returns a copy of the value associated with an index (or null_value) if no
  // value is associated with an index.
  const Value& GetValueOrDie(Index<Tag, V> index) const {
    CHECK(values_.size() > index);
    CHECK(values_[index].first);
    return values_[index].second;
  }

  Value& GetValueOrDie(Index<Tag, V> index) {
    CHECK(values_.size() > index);
    CHECK(values_[index].first);
    return values_[index].second;
  }

  bool HasValue(Index<Tag, V> index) const {
    if (values_.size() > index) {
      return values_[index].first;
    }

    return false;
  }

  Value& operator[](Index<Tag, V> index) {
    values_.resize(std::max(values_.size(), index + 1));
    std::pair<bool, Value>& bool_and_value = values_[index];
    bool_and_value.first = true;
    return bool_and_value.second;
  }

  Value& UnsafeAccess(Index<Tag, V> index) {
    std::pair<bool, Value>& bool_and_value = values_[index];
    bool_and_value.first = true;
    return bool_and_value.second;
  }

  const Value& UnsafeAccess(Index<Tag, V> index) const {
    return values_[index].second;
  }

  void Resize(size_t size) { values_.resize(size); }

  void Clear() { values_.clear(); }

  const Value& operator[](Index<Tag, V> index) const {
    return GetValueOrDie(index);
  }

  size_t Count() const {
    return std::count_if(
        values_.begin(), values_.end(),
        [](const std::pair<bool, Value>& pair) { return pair.first; });
  };

  bool Empty() const { return Count() == 0; }

  Iterator begin() { return {this, Index<Tag, V>(GetFirst())}; }
  Iterator end() { return {this, Index<Tag, V>(values_.size())}; }

  ConstIterator begin() const { return {this, Index<Tag, V>(GetFirst())}; }
  ConstIterator end() const { return {this, Index<Tag, V>(values_.size())}; }

  friend bool operator<(const PerfectHashMap<V, Tag, Value>& a,
                        const PerfectHashMap<V, Tag, Value>& b) {
    return a.values_ < b.values_;
  }

  friend bool operator==(const PerfectHashMap<V, Tag, Value>& a,
                         const PerfectHashMap<V, Tag, Value>& b) {
    return a.values_ == b.values_;
  }

 private:
  Index<Tag, V> GetFirst() const {
    size_t i;
    for (i = 0; i < values_.size(); ++i) {
      if (values_[i].first) {
        break;
      }
    }

    return Index<Tag, V>(i);
  }

  std::vector<std::pair<bool, Value>> values_;
};
}

#endif
