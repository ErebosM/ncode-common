#include <stddef.h>
#include <chrono>
#include <iostream>
#include <memory>
#include <type_traits>
#include <vector>

#include "common.h"
#include "free_list.h"

struct Dummy {
  Dummy(double a1, double a2) : a1(a1), a2(a2) {}

  double a1;
  double a2;
};

using namespace std::chrono;
using DummyPtr = nc::FreeList<Dummy>::Pointer;
static constexpr size_t kPasses = 5000;

static uint64_t TestStandardAllocation() {
  auto start = high_resolution_clock::now();
  for (size_t i = 0; i < kPasses; ++i) {
    std::vector<std::unique_ptr<Dummy>> values(i);
    for (size_t j = 0; j < i; ++j) {
      auto dummy_value = nc::make_unique<Dummy>(i, j);
      values[j] = std::move(dummy_value);
    }
  }
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  return duration.count();
}

static uint64_t TestFreeList() {
  nc::FreeList<Dummy>& free_list = nc::GetFreeList<Dummy>();
  auto start = high_resolution_clock::now();
  for (size_t i = 0; i < kPasses; ++i) {
    std::vector<DummyPtr> values(i);
    for (size_t j = 0; j < i; ++j) {
      auto dummy_value = free_list.New(i, j);
      values[j] = std::move(dummy_value);
    }
  }
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  return duration.count();
}

static uint64_t TestUnsafeFreeList() {
  nc::UnsafeFreeList<Dummy>& free_list = nc::GetUnsafeFreeList<Dummy>();
  auto start = high_resolution_clock::now();
  for (size_t i = 0; i < kPasses; ++i) {
    std::vector<DummyPtr> values(i);
    for (size_t j = 0; j < i; ++j) {
      auto dummy_value = free_list.New(i, j);
      values[j] = std::move(dummy_value);
    }
  }
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);
  return duration.count();
}


int main(int argc, char** argv) {
  nc::Unused(argc);
  nc::Unused(argv);

  // Will compare the free list vs regular memory allocation.
  uint64_t regular_ms = TestStandardAllocation();
  uint64_t free_list_ms = TestFreeList();
  uint64_t free_list_unsafe_ms = TestUnsafeFreeList();

  std::cout << "Regular " << regular_ms << "ms\n";
  std::cout << "Free list " << free_list_ms << "ms\n";
  std::cout << "Free list (unsafe) " << free_list_unsafe_ms << "ms\n";
}
