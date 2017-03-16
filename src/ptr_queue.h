#ifndef NCODE_PTR_QUEUE_H
#define NCODE_PTR_QUEUE_H

#include <functional>
#include <mutex>
#include <array>
#include <condition_variable>

#include "common.h"
#include "logging.h"

namespace nc {

// A thread-safe blocking queue for passing pointers between threads.
template <typename T, size_t Size, typename Deleter = std::default_delete<T>>
class PtrQueue {
 public:
  typedef std::function<bool(const T& item)> InvalidateCallback;

  // The size of the queue
  static constexpr size_t kQueueSize = Size;

  PtrQueue() : producer_(0), consumer_(0), num_items_(0), closed_(false) {
    static_assert(IsPowerOfTwo(Size), "Queue size must be a power of 2");
  }

  ~PtrQueue() { Close(); }

  // Returns the number of items in the queue (including invalid ones)
  size_t size() {
    std::unique_lock<std::mutex> lock(mu_);
    return num_items_;
  }

  bool empty() { return size() == 0; }

  // Will produce. If the queue is already closed or if the produce action
  // blocks and the queue is closed this method is a no-op and the item will be
  // lost (freed) in this case false is returned.
  bool ProduceOrBlockWithTimeout(std::unique_ptr<T, Deleter> item,
                                 std::chrono::milliseconds timeout,
                                 bool* timed_out) {
    *timed_out = false;
    std::unique_lock<std::mutex> lock(mu_);
    if (closed_) {
      return false;
    }

    if (num_items_ == Size) {
      bool p = condition_.wait_for(
          lock, timeout, [this] { return closed_ || num_items_ < Size; });
      if (!p) {
        *timed_out = true;
        return false;
      }

      if (closed_) {
        return false;
      }
    }

    queue_[producer_] = std::move(item);
    producer_ = (producer_ + 1) & kMask;

    num_items_++;
    condition_.notify_all();
    return true;
  }

  // Same as above, no timeout.
  bool ProduceOrBlock(std::unique_ptr<T, Deleter> item) {
    bool dummy;
    return ProduceOrBlockWithTimeout(std::move(item),
                                     std::chrono::milliseconds::max(), &dummy);
  }

  // Will consume. If the queue is closed and empty it will return an empty
  // unique_ptr. Can also specify timeout.
  std::unique_ptr<T, Deleter> ConsumeOrBlockWithTimeout(
      std::chrono::milliseconds timeout, bool* timed_out) {
    std::unique_ptr<T, Deleter> return_ptr;
    *timed_out = false;

    {
      std::unique_lock<std::mutex> lock(mu_);
      while (return_ptr.get() == nullptr) {
        if (num_items_ == 0) {
          if (closed_) {
            return std::move(return_ptr);
          }

          bool p = condition_.wait_for(
              lock, timeout, [this] { return closed_ || num_items_ > 0; });
          if (!p) {
            *timed_out = true;
            return std::move(return_ptr);
          }

          if (closed_ && num_items_ == 0) {
            return std::move(return_ptr);
          }
        }

        return_ptr = std::move(queue_[consumer_]);
        consumer_ = (consumer_ + 1) & kMask;
        num_items_--;
      }

      condition_.notify_all();
      if (return_ptr.get() != nullptr) {
        return std::move(return_ptr);
      }
    }

    // If all items were invalid we should try again.
    return ConsumeOrBlockWithTimeout(timeout, timed_out);
  }

  // Same as above, but no timeout.
  std::unique_ptr<T, Deleter> ConsumeOrBlock() {
    bool dummy;
    return ConsumeOrBlockWithTimeout(std::chrono::milliseconds::max(), &dummy);
  }

  // Invalidates all items for which the callback evaluates to true
  void Invalidate(InvalidateCallback callback) {
    std::unique_lock<std::mutex> lock(mu_);

    for (size_t i = 0; i < Size; i++) {
      if (queue_[i].get() != nullptr) {
        if (callback(*queue_[i].get())) {
          queue_[i].reset();
        }
      }
    }
  }

  // After this call no more items can be produced.
  void Close() {
    std::unique_lock<std::mutex> lock(mu_);
    closed_ = true;
    condition_.notify_all();
  }

 private:
  static constexpr size_t kMask = Size - 1;

  // Index of the next element to produce
  size_t producer_;

  // Index of the next element to consume
  size_t consumer_;

  // Number of items in the queue
  size_t num_items_;

  // Can items be added to the queue
  bool closed_;

  // A circular buffer.
  std::array<std::unique_ptr<T, Deleter>, Size> queue_;

  // Something for consumers / producers to wait on.
  std::condition_variable condition_;

  // Mutex for the condition variable above.
  std::mutex mu_;

  DISALLOW_COPY_AND_ASSIGN(PtrQueue);
};

}  // namespace nc

#endif /* NCODE_PTR_QUEUE_H */
