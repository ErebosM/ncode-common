#ifndef NCODE_FREE_LIST_H
#define NCODE_FREE_LIST_H

#include <stddef.h>
#include <vector>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include "common.h"
#include "logging.h"

namespace nc {

template <typename T>
class FreeList;

template <typename T>
FreeList<T>& GetFreeList();

// A free list that amortizes the new/delete cost for objects by never releasing
// memory to the OS. This class is thread-safe.
template <typename T>
class FreeList {
 public:
  typedef std::unique_ptr<T, std::function<void(void*)>> Pointer;

  // After how many raw allocations a thread will check to see if the global
  // free list contains any memory.
  static constexpr uint64_t kRawAllocationThreshold = 16ul;

  // If a thread local free list contains this many elements half of them will
  // be moved to the global free list.
  static constexpr uint64_t kMoveToGlobalThreshold = 1000ul;

  // How many objects to allocate at once.
  static constexpr uint64_t kBatchSize = 16ul;

  ~FreeList() {
    std::lock_guard<std::mutex> lock(mu_);
    --free_lists_count_;
    for (T* mem : to_free_) {
      std::free(mem);
    }
  }

  void Release(void* raw_ptr) {
    static_cast<T*>(raw_ptr)->~T();
    objects_.emplace_back(static_cast<T*>(raw_ptr));

    if (objects_.size() >= kMoveToGlobalThreshold) {
      std::lock_guard<std::mutex> lock(mu_);
      size_t count = objects_.size() / 2;
      global_free_objects_.insert(global_free_objects_.end(),
                                  objects_.end() - count, objects_.end());
      objects_.resize(objects_.size() - count);
    }
  }

  static void ReleaseGlobal(void* ptr) { GetFreeList<T>().Release(ptr); }

  template <typename... Args>
  Pointer New(Args&&... args) {
    if (objects_.empty()) {
      size_t count = 0;
      if (raw_allocation_count_ % kRawAllocationThreshold == 0) {
        std::lock_guard<std::mutex> lock(mu_);
        // Will steal 1/N of all elements in the global free list where N is the
        // number of thread-local free lists.
        count = global_free_objects_.size() / free_lists_count_;
        if (count > 0) {
          objects_.insert(objects_.end(), global_free_objects_.end() - count,
                          global_free_objects_.end());
          global_free_objects_.resize(global_free_objects_.size() - count);
        }
      }

      if (count == 0) {
        T* mem = static_cast<T*>(std::malloc(kBatchSize * sizeof(T)));
        to_free_.emplace_back(mem);
        for (size_t i = 0; i < kBatchSize - 1; ++i) {
          objects_.emplace_back(&(mem[i]));
        }

        T* const raw_ptr = &(mem[kBatchSize - 1]);
        new (raw_ptr) T(std::forward<Args>(args)...);
        ++raw_allocation_count_;
        return Pointer(raw_ptr, &ReleaseGlobal);
      }
    }

    T* const raw_ptr = objects_.back();
    objects_.pop_back();

    new (raw_ptr) T(std::forward<Args>(args)...);
    return Pointer(raw_ptr, &ReleaseGlobal);
  }

  // Returns the number of objects that this free list holds.
  size_t NumObjects() const { return objects_.size(); }

 private:
  FreeList() : raw_allocation_count_(0) {
    std::lock_guard<std::mutex> lock(mu_);
    ++free_lists_count_;
  }

  // Number of objects allocated.
  uint64_t raw_allocation_count_;

  // Free objects that can be assigned when needed.
  std::vector<T*> objects_;

  // The free list only releases memory upon destruction. This list stores
  // pointers to the chunks of memory allocated from the OS (as opposed to
  // objects stored in 'objects_') so that we can free them upon destruction.
  std::vector<T*> to_free_;

  // A scratch list of objects that can be taken by any thread.
  static std::vector<T*> global_free_objects_;

  // Keeps track of all free lists.
  static size_t free_lists_count_;

  // Protects all_free_lists_.
  static std::mutex mu_;

  friend FreeList& GetFreeList<T>();

  DISALLOW_COPY_AND_ASSIGN(FreeList);
};

template <typename T>
size_t FreeList<T>::free_lists_count_;

template <typename T>
std::mutex FreeList<T>::mu_;

template <typename T>
std::vector<T*> FreeList<T>::global_free_objects_;

// Returns a global singleton free list instance for a type.
template <typename T>
FreeList<T>& GetFreeList() {
  static thread_local FreeList<T>* free_list = new FreeList<T>();
  return *free_list;
}

// Allocates an object from the singleton free list for a type.
template <typename T, typename... Args>
typename FreeList<T>::Pointer AllocateFromFreeList(Args&&... args) {
  return GetFreeList<T>().New(std::forward<Args>(args)...);
}

template <typename T>
class UnsafeFreeList;

template <typename T>
UnsafeFreeList<T>& GetUnsafeFreeList();

// An unsafe variant of FreeList. Faster, but it is up to the user to make sure
// the same thread frees objects as the one that allocates them.
template <typename T>
class UnsafeFreeList {
 public:
  typedef std::unique_ptr<T, std::function<void(void*)>> Pointer;

  // How many objects to allocate at once.
  static constexpr uint64_t kBatchSize = 32ul;

  ~UnsafeFreeList() {
    for (T* mem : to_free_) {
      std::free(mem);
    }
  }

  void Release(void* raw_ptr) {
    static_cast<T*>(raw_ptr)->~T();
    objects_.emplace_back(static_cast<T*>(raw_ptr));
  }

  template <typename... Args>
  Pointer New(Args&&... args) {
    T* raw_ptr;
    if (objects_.empty()) {
      T* mem = static_cast<T*>(std::malloc(kBatchSize * sizeof(T)));
      to_free_.emplace_back(mem);

      for (size_t i = 0; i < kBatchSize - 1; ++i) {
        objects_.emplace_back(&(mem[i]));
      }

      raw_ptr = &(mem[kBatchSize - 1]);
    } else {
      raw_ptr = objects_.back();
      objects_.pop_back();
    }

    new (raw_ptr) T(std::forward<Args>(args)...);
    return Pointer(raw_ptr, [this](void* ptr) { Release(ptr); });
  }

  // Returns the number of objects that this free list holds.
  size_t NumObjects() const { return objects_.size(); }

  UnsafeFreeList() {}

 private:
  // Free objects that can be assigned when needed.
  std::vector<T*> objects_;

  // The free list only releases memory upon destruction.
  std::vector<T*> to_free_;

  friend UnsafeFreeList& GetUnsafeFreeList<T>();

  DISALLOW_COPY_AND_ASSIGN(UnsafeFreeList);
};

// Returns a global singleton free list instance for a type.
template <typename T>
UnsafeFreeList<T>& GetUnsafeFreeList() {
  static UnsafeFreeList<T>* free_list = new UnsafeFreeList<T>();
  return *free_list;
}

// Allocates an object from the singleton free list for a type.
template <typename T, typename... Args>
typename UnsafeFreeList<T>::Pointer AllocateFromUnsafeFreeList(Args&&... args) {
  return GetUnsafeFreeList<T>().New(std::forward<Args>(args)...);
}

}  // namespace nc

#endif
