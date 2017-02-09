#ifndef NCODE_COMMON_THREAD_RUNNER_H
#define NCODE_COMMON_THREAD_RUNNER_H

#include <stddef.h>
#include <functional>
#include <thread>
#include <vector>
#include <mutex>

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

}  // namespace common

#endif
