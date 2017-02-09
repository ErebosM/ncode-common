#include "perfect_hash.h"

#include "gtest/gtest.h"

namespace nc {
namespace {

struct ItemTag {};
using Store = PerfectHashStore<std::string, uint8_t, ItemTag>;
using Set = PerfectHashSet<uint8_t, ItemTag>;
using Map = PerfectHashMap<uint8_t, ItemTag, std::string>;

TEST(PerfectHash, Store) {
  Store store;

  std::string item = "SomeItem";
  auto index = store.AddItem(item);
  ASSERT_EQ(item, store.GetItemOrDie(index));

  Index<ItemTag, uint8_t> other_index(1);
  ASSERT_DEATH(store.GetItemOrDie(other_index), ".*");
  ASSERT_EQ(nullptr, store.GetItemOrNull(other_index));
}

TEST(PerfectHash, StoreTooManyItems) {
  Store store;
  for (size_t i = 0; i < 255; ++i) {
    store.AddItem(std::to_string(i));
  }

  ASSERT_DEATH(store.AddItem(std::to_string(256)), ".*");
}

TEST(PerfectHash, Set) {
  Store store;

  std::string item = "SomeItem";
  auto index = store.AddItem(item);

  std::string other_item = "OtherItem";
  auto other_index = store.AddItem(other_item);

  Set set;
  ASSERT_EQ(0ul, set.Count());
  ASSERT_FALSE(set.Contains(index));
  ASSERT_FALSE(set.Contains(other_index));
  set.Insert(index);
  ASSERT_TRUE(set.Contains(index));
  ASSERT_FALSE(set.Contains(other_index));
  ASSERT_EQ(1ul, set.Count());
}

TEST(PerfectHash, Map) {
  Store store;
  auto index = store.AddItem("SomeItem");

  Map map;
  map[index] = "HI";

  ASSERT_EQ("HI", map[index]);
  ASSERT_EQ("HI", map.GetValueOrDie(index));

  auto other_index = store.AddItem("OtherItem");
  ASSERT_DEATH(map.GetValueOrDie(other_index), ".*");
  ASSERT_EQ("", map[other_index]);
}

TEST(PerfectHash, MapIter) {
  Map map;

  size_t i = 0;
  for (auto index_and_value : map) {
    Unused(index_and_value);
    ++i;
  }
  ASSERT_EQ(0ul, i);
  ASSERT_EQ(0ul, map.Count());

  Store store;
  auto index_one = store.AddItem("SomeItem1");
  auto index_two = store.AddItem("SomeItem2");
  auto index_three = store.AddItem("SomeItem3");
  Unused(index_two);

  map[index_three] = "A";
  for (auto index_and_value : map) {
    ASSERT_EQ(index_three, index_and_value.first);
    ASSERT_EQ("A", *index_and_value.second);
    ++i;
  }
  ASSERT_EQ(1ul, i);
  ASSERT_EQ(1ul, map.Count());

  i = 0;
  map[index_one] = "B";
  for (auto index_and_value : map) {
    if (i == 0) {
      ASSERT_EQ(index_one, index_and_value.first);
      ASSERT_EQ("B", *index_and_value.second);
    } else if (i == 1) {
      ASSERT_EQ(index_three, index_and_value.first);
      ASSERT_EQ("A", *index_and_value.second);
    }
    ++i;
  }
  ASSERT_EQ(2ul, i);
  ASSERT_EQ(2ul, map.Count());
}

TEST(PerfectHash, SetIter) {
  Set set;

  size_t i = 0;
  for (auto index : set) {
    Unused(index);
    ++i;
  }
  ASSERT_EQ(0ul, i);

  Store store;
  auto index_one = store.AddItem("SomeItem1");
  auto index_two = store.AddItem("SomeItem2");
  auto index_three = store.AddItem("SomeItem3");
  Unused(index_two);

  set.Insert(index_three);
  ASSERT_EQ(index_three, *set.begin());
  for (auto index : set) {
    ASSERT_EQ(index_three, index);
    ++i;
  }
  ASSERT_EQ(1ul, i);
  ASSERT_EQ(1ul, set.Count());

  i = 0;
  set.Insert(index_one);
  for (auto index : set) {
    if (i == 0) {
      ASSERT_EQ(index_one, index);
    } else if (i == 1) {
      ASSERT_EQ(index_three, index);
    }
    ++i;
  }
  ASSERT_EQ(2ul, i);
  ASSERT_EQ(2ul, set.Count());

  set.Remove(index_one);
  set.Remove(index_two);
  set.Remove(index_three);

  i = 0;
  for (auto index : set) {
    Unused(index);
    ++i;
  }
  ASSERT_EQ(0ul, i);
  ASSERT_EQ(0ul, set.Count());
}

TEST(PerfectHash, FullSet) {
  Store store;
  auto index_one = store.AddItem("SomeItem1");
  auto index_two = store.AddItem("SomeItem2");
  auto index_three = store.AddItem("SomeItem3");

  Set set = Set::FullSetFromStore(store);
  size_t i = 0;
  for (auto index : set) {
    if (i == 0) {
      ASSERT_EQ(index_one, index);
    } else if (i == 1) {
      ASSERT_EQ(index_two, index);
    } else if (i == 2) {
      ASSERT_EQ(index_three, index);
    }
    ++i;
  }
  ASSERT_EQ(3ul, i);
  ASSERT_EQ(3ul, set.Count());
}

struct OtherItemTag {};
using StoreNotCopyable =
    PerfectHashStore<std::unique_ptr<std::string>, uint8_t, OtherItemTag>;

TEST(PerfectHashNotCopyable, Compile) {
  StoreNotCopyable store;

  auto item = make_unique<std::string>("SomeItem");
  auto index = store.MoveItem(std::move(item));
  ASSERT_EQ("SomeItem", *store.GetItemOrDie(index));
}

}  // namespace
}  // namespace nc
