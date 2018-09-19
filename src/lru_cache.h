#ifndef NCODE_LRU_H
#define NCODE_LRU_H

#include <stddef.h>
#include <functional>
#include <list>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common.h"
#include "map_util.h"

namespace nc {

// An LRU cache that maps K to V.
template <typename K, typename V, class Hash = std::hash<K>,
          class Pred = std::equal_to<K>>
class LRUCache {
 public:
  virtual ~LRUCache() {}
  LRUCache(size_t max_cache_size) : max_cache_size_(max_cache_size) {}

  // Inserts a new item in the cache, or inserts a new one with the given
  // arguments to the constructor. This call will only result in a constructor
  // being called if there is no entry associated with 'key'.
  template <class... Args>
  V& Emplace(const K& key, Args&&... args) {
    ObjectAndListIterator& object_and_list_it = cache_map_[key];
    if (object_and_list_it.object) {
      // No insertion took place, key was already in cache. Will just update
      // keys_.
      typename LRUList::iterator list_it = object_and_list_it.iterator;
      keys_.splice(keys_.begin(), keys_, list_it);
      return *object_and_list_it.object;
    }

    if (cache_map_.size() > max_cache_size_) {
      EvictOldest();
    }

    // Insertion took place. Need to add the key to the front of the list and
    // actually construct the value.
    keys_.emplace_front(key);
    object_and_list_it.object = make_unique<V>(args...);
    object_and_list_it.iterator = keys_.begin();
    return *object_and_list_it.object;
  }

  // Like Emplace, but always constructs a new entry and evicts the entry that
  // has the same key (if any).
  template <class... Args>
  V& InsertNew(const K& key, Args&&... args) {
    return InsertNew(key, make_unique<V>(args...));
  }

  // Inserts a new entry and evicts the entry that has the same key (if any).
  V& InsertNew(const K& key, std::unique_ptr<V> value) {
    ObjectAndListIterator& object_and_list_it = cache_map_[key];
    if (object_and_list_it.object) {
      // Will evict the current entry.
      std::unique_ptr<V> to_evict_value = std::move(object_and_list_it.object);
      ItemEvicted(key, std::move(to_evict_value));

      typename LRUList::iterator list_it = object_and_list_it.iterator;
      keys_.splice(keys_.begin(), keys_, list_it);
    } else {
      // A new entry was created -- need to check if we have overstepped the
      // size limit.
      if (cache_map_.size() > max_cache_size_) {
        EvictOldest();
      }

      keys_.emplace_front(key);
      object_and_list_it.iterator = keys_.begin();
    }

    object_and_list_it.object = std::move(value);
    return *object_and_list_it.object;
  }

  V* FindOrNull(const K& key) {
    ObjectAndListIterator* object_and_list_it =
        ::nc::FindOrNull(cache_map_, key);
    if (object_and_list_it == nullptr) {
      return nullptr;
    }

    CHECK(object_and_list_it->object);
    typename LRUList::iterator list_it = object_and_list_it->iterator;
    keys_.splice(keys_.begin(), keys_, list_it);
    return object_and_list_it->object.get();
  }

  // Evicts the entire cache.
  void EvictAll() {
    while (cache_map_.size()) {
      EvictOldest();
    }
  }

  // Called when an item is evicted from the cache.
  virtual void ItemEvicted(const K& key, std::unique_ptr<V> value) {
    Unused(key);
    Unused(value);
  };

  std::unordered_map<K, const V*, Hash, Pred> Values() const {
    std::unordered_map<K, const V*, Hash, Pred> out;
    for (const auto& key_and_rest : cache_map_) {
      const K& key = key_and_rest.first;
      const ObjectAndListIterator& rest = key_and_rest.second;
      out[key] = rest.object.get();
    }

    return out;
  }

 private:
  using LRUList = std::list<K>;
  struct ObjectAndListIterator {
    ObjectAndListIterator() {}

    std::unique_ptr<V> object;
    typename LRUList::iterator iterator;
  };
  using CacheMap = std::unordered_map<K, ObjectAndListIterator, Hash, Pred>;

  void EvictOldest() {
    // Need to evict an entry.
    const K& to_evict = keys_.back();
    auto it = cache_map_.find(to_evict);
    CHECK(it != cache_map_.end());

    std::unique_ptr<V> to_evict_value = std::move(it->second.object);
    ItemEvicted(to_evict, std::move(to_evict_value));

    cache_map_.erase(it);
    keys_.pop_back();
  }

  const size_t max_cache_size_;

  LRUList keys_;
  CacheMap cache_map_;
};

}  // namespace nc

#endif
