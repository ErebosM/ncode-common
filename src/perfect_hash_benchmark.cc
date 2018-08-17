#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <set>
#include <vector>

#include "common.h"
#include "logging.h"
#include "perfect_hash.h"

struct ItemTag {};
using Store = nc::PerfectHashStore<uint32_t, uint8_t, ItemTag>;
using Set = nc::PerfectHashSet<uint8_t, ItemTag>;
using Map = nc::PerfectHashMap<uint8_t, ItemTag, std::string>;

static constexpr size_t kIter = 100000000ul;
static constexpr size_t kNumKeys = 255;

using namespace std::chrono;

static void TimeMs(const std::string& msg, std::function<void()> f) {
  auto start = high_resolution_clock::now();
  f();
  auto end = high_resolution_clock::now();
  auto duration_std = duration_cast<milliseconds>(end - start);
  LOG(INFO) << msg << " :" << duration_std.count() << "ms";
}

size_t count = 0;
int main(int argc, char** argv) {
  nc::Unused(argc);
  nc::Unused(argv);

  Store store;

  std::vector<Store::IndexType> indices;
  std::vector<uint32_t> keys;
  for (size_t i = 0; i < kNumKeys; ++i) {
    indices.emplace_back(store.AddItem(i));
    keys.emplace_back(i);
  }

  std::set<uint32_t> regular_set;
  Set ph_set;

  TimeMs("Standard set insert", [&regular_set, &keys] {
    for (size_t i = 0; i < kIter; ++i) {
      regular_set.insert(keys[i % kNumKeys]);
    }
  });

  TimeMs("PH set insert", [&ph_set, &indices] {
    for (size_t i = 0; i < kIter; ++i) {
      ph_set.Insert(indices[i % kNumKeys]);
    }
  });

  TimeMs("Standard set get", [&regular_set, &keys] {
    for (size_t i = 0; i < kIter; ++i) {
      count += regular_set.count(keys[i % kNumKeys]);
    }
  });

  TimeMs("PH set get", [&ph_set, &indices] {
    for (size_t i = 0; i < kIter; ++i) {
      count += ph_set.Contains(indices[i % kNumKeys]);
    }
  });
}
