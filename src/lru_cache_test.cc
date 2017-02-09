#include "lru_cache.h"

#include <thread>
#include "gtest/gtest.h"

namespace nc {
namespace {

static constexpr size_t kCacheSize = 1000;

class CacheForTest : public LRUCache<int, double> {
 public:
  CacheForTest() : LRUCache<int, double>(kCacheSize) {}

  void ItemEvicted(const int& key, std::unique_ptr<double> value_ptr) {
    evicted_items_.emplace_back(key, *value_ptr);
  }

  const std::vector<std::pair<int, double>>& evicted_items() {
    return evicted_items_;
  }

 private:
  std::vector<std::pair<int, double>> evicted_items_;
};

class CacheTest : public ::testing::Test {
 protected:
  CacheForTest cache_;
};

TEST_F(CacheTest, Empty) {
  ASSERT_TRUE(cache_.evicted_items().empty());
  cache_.EvictAll();
  ASSERT_TRUE(cache_.evicted_items().empty());
}

TEST_F(CacheTest, UpToSize) {
  for (size_t i = 0; i < kCacheSize; ++i) {
    cache_.Emplace(i, 10.0 + i);
  }
  ASSERT_TRUE(cache_.evicted_items().empty());

  for (size_t i = 0; i < kCacheSize; ++i) {
    // This will not update the value, as it already exists.
    ASSERT_EQ(10.0 + i, cache_.Emplace(i, 1.0));
  }

  for (size_t i = 0; i < kCacheSize; ++i) {
    ASSERT_EQ(10.0 + i, *cache_.FindOrNull(i));
  }
  ASSERT_TRUE(cache_.evicted_items().empty());
}

TEST_F(CacheTest, LeastRecent) {
  for (size_t i = 0; i < kCacheSize; ++i) {
    cache_.Emplace(i, 10.0 + i);
  }

  cache_.Emplace(kCacheSize, 10.0 + kCacheSize);
  std::vector<std::pair<int, double>> model = {{0, 10.0}};
  ASSERT_EQ(model, cache_.evicted_items());
}

TEST_F(CacheTest, SecondLeastRecent) {
  for (size_t i = 0; i < kCacheSize; ++i) {
    cache_.Emplace(i, 10.0 + i);
  }

  // This will make key 0 the most recently used.
  cache_.Emplace(0, 10.0 + kCacheSize);

  cache_.Emplace(kCacheSize, 10.0 + kCacheSize);
  std::vector<std::pair<int, double>> model = {{1, 11.0}};
  ASSERT_EQ(model, cache_.evicted_items());
}

TEST_F(CacheTest, EvictAll) {
  for (size_t i = 0; i < kCacheSize; ++i) {
    cache_.Emplace(i, 10.0 + i);
  }
  cache_.Emplace(0, 0.0);

  cache_.EvictAll();
  std::vector<std::pair<int, double>> model;
  for (size_t i = 0; i < kCacheSize - 1; ++i) {
    model.emplace_back(i + 1, 10.0 + i + 1);
  }
  model.emplace_back(0, 10.0);
  ASSERT_EQ(model, cache_.evicted_items());
}

TEST_F(CacheTest, InsertNew) {
  cache_.Emplace(10, 10.0);
  cache_.InsertNew(10, 11.0);
  cache_.EvictAll();

  std::vector<std::pair<int, double>> model = {{10, 10.0}, {10, 11.0}};
  ASSERT_EQ(model, cache_.evicted_items());
}

TEST_F(CacheTest, LeastRecentInsertNew) {
  for (size_t i = 0; i < kCacheSize; ++i) {
    cache_.Emplace(i, 10.0 + i);
  }

  cache_.InsertNew(0, 1000.0);
  cache_.Emplace(kCacheSize, 10.0 + kCacheSize);

  // The call to InsertNew should have evicted (0,10.0) and replaced it with (0,
  // 1000.0). The call to Emplace then should have overflown the cache and the
  // least recently accessed item (1, 11.0) should have been evicted.
  std::vector<std::pair<int, double>> model = {{0, 10.0}, {1, 11.0}};
  ASSERT_EQ(model, cache_.evicted_items());
}

TEST_F(CacheTest, LeastRecentInsertNewOverflow) {
  for (size_t i = 0; i < kCacheSize; ++i) {
    cache_.InsertNew(i, 10.0 + i);
  }

  cache_.InsertNew(kCacheSize, 10.0 + kCacheSize);
  std::vector<std::pair<int, double>> model = {{0, 10.0}};
  ASSERT_EQ(model, cache_.evicted_items());
}

struct CompositeValue {
  CompositeValue(size_t a, double b) : a(a), b(b) {}

  size_t a;
  double b;
};

TEST(CompileTest, CompositeValue) {
  LRUCache<int, CompositeValue> cache(kCacheSize);
  cache.Emplace(1, 2ul, 3.0);
  cache.EvictAll();
}

}  // namespace
}  // namespace nc
