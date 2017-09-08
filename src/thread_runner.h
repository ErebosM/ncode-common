#ifndef NCODE_COMMON_THREAD_RUNNER_H
#define NCODE_COMMON_THREAD_RUNNER_H

#include <stddef.h>
#include <functional>
#include <thread>
#include <vector>
#include <condition_variable>
#include <mutex>
#include <atomic>

namespace nc {

// Runs instances of a given function in parallel. At any given moment in time
// up to 'batch_size' function will run in parallel. This function will block
// and return when all functions have completed.
template <typename T>
void RunInParallel(const std::vector<T>& arguments,
                   std::function<void(const T&)> f, size_t batch = 4) {
  CHECK(batch > 0) << "Zero batch size";

  std::mutex mu;
  std::vector<bool> done(arguments.size(), false);

  std::vector<std::thread> threads;
  for (size_t j = 0; j < batch; ++j) {
    threads.emplace_back([&arguments, &f, &mu, &done] {
      mu.lock();
      for (size_t i = 0; i < arguments.size(); ++i) {
        if (!done[i]) {
          done[i] = true;
          mu.unlock();
          f(arguments[i]);
          mu.lock();
        }
      }
      mu.unlock();
    });
  }

  for (size_t i = 0; i < batch; ++i) {
    threads[i].join();
  }
}

template <typename T, typename Result>
std::vector<std::unique_ptr<Result>> RunInParallelWithResult(
    const std::vector<T>& arguments,
    std::function<std::unique_ptr<Result>(const T&)> f, size_t batch = 4) {
  CHECK(batch > 0) << "Zero batch size";

  std::mutex mu;
  std::vector<bool> done(arguments.size(), false);
  std::vector<std::unique_ptr<Result>> results(arguments.size());

  std::vector<std::thread> threads;
  for (size_t j = 0; j < batch; ++j) {
    threads.emplace_back([&arguments, &f, &mu, &done, &results] {
      mu.lock();
      for (size_t i = 0; i < arguments.size(); ++i) {
        if (!done[i]) {
          done[i] = true;
          mu.unlock();
          std::unique_ptr<Result> result = f(arguments[i]);
          results[i] = std::move(result);
          mu.lock();
        }
      }
      mu.unlock();
    });
  }

  for (size_t i = 0; i < batch; ++i) {
    threads[i].join();
  }

  return results;
}

// Runs and maintains a number of threads that process incoming data. Each
// thread can be associated with an instance of Data.
template <typename T>
class ThreadBatchProcessor {
 public:
  ThreadBatchProcessor(size_t threads)
      : thread_count_(threads),
        to_kill_(false),
        batch_arguments_(nullptr),
        batch_f_(nullptr),
        number_active_(0) {
    active_threads_.resize(thread_count_, false);
    for (size_t i = 0; i < threads; ++i) {
      threads_.emplace_back([this, i] { DoWork(i); });
    }
  }

  ~ThreadBatchProcessor() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      to_kill_ = true;
      new_batch_ready_.notify_all();
    }

    for (size_t i = 0; i < thread_count_; ++i) {
      threads_[i].join();
    }
  }

  void RunInParallel(const std::vector<T>& arguments,
                     std::function<void(const T&, size_t, size_t)> f) {
    {
      std::unique_lock<std::mutex> lock(mu_);
      batch_arguments_ = &arguments;
      batch_f_ = &f;

      batch_done_.resize(arguments.size(), false);

      // Activate all threads.
      std::fill(active_threads_.begin(), active_threads_.end(), true);
      number_active_ = thread_count_;
    }
    new_batch_ready_.notify_all();
    {
      std::unique_lock<std::mutex> lock(mu_);
      thread_done_.wait(lock, [this] { return number_active_ == 0; });

      batch_arguments_ = nullptr;
      batch_f_ = nullptr;
      batch_done_.clear();

      // Deactivate all threads.
      std::fill(active_threads_.begin(), active_threads_.end(), false);
    }
  }

 private:
  void DoWork(size_t thread_index) {
    while (!to_kill_) {
      std::unique_lock<std::mutex> lock(mu_);
      new_batch_ready_.wait(lock, [this, thread_index] {
        return to_kill_ || active_threads_[thread_index];
      });
      if (to_kill_) {
        break;
      }

      const std::vector<T>& arguments = *batch_arguments_;
      const std::function<void(const T&, size_t, size_t)>& f = *batch_f_;

      for (size_t i = 0; i < arguments.size(); ++i) {
        if (!batch_done_[i]) {
          batch_done_[i] = true;
          lock.unlock();
          f(arguments[i], i, thread_index);
          lock.lock();
        }
      }

      active_threads_[thread_index] = false;
      --number_active_;
      if (number_active_ == 0) {
        lock.unlock();
        thread_done_.notify_one();
      }
    }
  }

  // Number of threads.
  size_t thread_count_;

  std::atomic<bool> to_kill_;

  // Done status of the current batch.
  std::vector<bool> batch_done_;

  // The arguments for the current batch.Either null if no batch, or points to
  // the stack of RunInParallel.
  const std::vector<T>* batch_arguments_;

  // Function for the current batch.
  std::function<void(const T&, size_t, size_t)>* batch_f_;

  // The processors.
  std::vector<std::thread> threads_;

  std::condition_variable new_batch_ready_;
  std::condition_variable thread_done_;

  // Number of threads active and a vector indicating which threads are active.
  size_t number_active_;
  std::vector<bool> active_threads_;

  // Mutex for all the condition variables.
  std::mutex mu_;
};

}  // namespace common

#endif
