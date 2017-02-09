#include "free_list.h"

#include <thread>
#include "gtest/gtest.h"

namespace nc {
namespace test {

static constexpr size_t kBatch = 100000;
static constexpr size_t kThreads = 10;

struct Dummy {
  Dummy(double field, std::function<void()> on_destruct = [] {})
      : field(field), on_destruct(on_destruct) {}
  ~Dummy() { on_destruct(); }
  double field;
  std::function<void()> on_destruct;
};

struct D1 : public Dummy {};
TEST(FreeListTest, Empty) { ASSERT_EQ(0ul, GetFreeList<D1>().NumObjects()); }

struct D2 : public Dummy {
  using Dummy::Dummy;
};
TEST(FreeListTest, SingleObject) {
  FreeList<Dummy>::Pointer ptr = GetFreeList<D2>().New(42.0);
  ASSERT_EQ(42.0, ptr->field);
  Dummy* ptr_before = ptr.get();

  // The new object should occupy the same memory as the previous one.
  ptr.reset();
  ptr = GetFreeList<D2>().New(43.0);
  ASSERT_EQ(ptr_before, ptr.get());
  ASSERT_EQ(43.0, ptr->field);
}

struct D3 : public Dummy {
  using Dummy::Dummy;
};
TEST(FreeListTest, DestructorCalled) {
  bool tmp = false;
  FreeList<Dummy>::Pointer ptr =
      GetFreeList<D3>().New(42.0, [&tmp] { tmp = true; });
  ptr.reset();
  ASSERT_TRUE(tmp);
}

struct D4 : public Dummy {
  using Dummy::Dummy;
};
TEST(FreeListSingleton, CanAllocate) {
  auto dummy_ptr = AllocateFromFreeList<D4>(12.0);
  ASSERT_NE(nullptr, dummy_ptr.get());
  ASSERT_EQ(12.0, dummy_ptr->field);
}

struct D5 : public Dummy {
  using Dummy::Dummy;
};
TEST(FreeListSingleton, MultiThread) {
  std::vector<std::unique_ptr<std::thread>> threads(kThreads);
  for (size_t i = 0; i < kThreads; ++i) {
    threads[i] = make_unique<std::thread>([i]() {
      for (size_t j = 0; j < kBatch; ++j) {
        AllocateFromFreeList<D5>(i * kBatch);
      }
    });
  }

  for (size_t i = 0; i < kThreads; ++i) {
    threads[i]->join();
  }
}

struct Base {
  Base(std::function<void()> on_destruct) : on_destruct_base(on_destruct) {}
  ~Base() { on_destruct_base(); }
  std::function<void()> on_destruct_base;
};

struct Derived : Base {
  Derived(std::function<void()> on_destruct_base,
          std::function<void()> on_destruct_derived)
      : Base(on_destruct_base), on_destruct_derived(on_destruct_derived) {}
  ~Derived() { on_destruct_derived(); }
  std::function<void()> on_destruct_derived;
};

TEST(FreeListSingleton, Hieararchy) {
  bool base = false;
  bool derived = false;
  auto dummy_ptr = AllocateFromFreeList<Derived>(
      [&base] { base = true; }, [&derived] { derived = true; });
  FreeList<Base>::Pointer base_ptr = std::move(dummy_ptr);
  base_ptr.reset();
  ASSERT_TRUE(base);
  ASSERT_TRUE(derived);
}

}  // namespace
}  // namespace nc
