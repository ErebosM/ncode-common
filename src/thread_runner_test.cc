#include <mutex>

#include "common.h"
#include "gtest/gtest.h"
#include "thread_runner.h"

namespace nc {
namespace {

TEST(ThreadRunnerTest, ZeroBatchSize) {
  ASSERT_DEATH(RunInParallel<int>({1}, [](int i) { Unused(i); }, 0), ".*");
}

class ThreadRunnerTestWithBatchSize : public ::testing::TestWithParam<int> {};

TEST_P(ThreadRunnerTestWithBatchSize, SimpleIO) {
  std::mutex mu;

  std::vector<int> args(20);
  std::iota(args.begin(), args.end(), 0);

  std::set<int> out;
  RunInParallel<int>(args,
                     [&out, &mu](int i) {
                       std::lock_guard<std::mutex> lock(mu);
                       out.insert(i);
                     },
                     GetParam());

  std::set<int> model(args.begin(), args.end());
  ASSERT_EQ(model, out);
}

INSTANTIATE_TEST_CASE_P(SimpleThreadRunner, ThreadRunnerTestWithBatchSize,
                        ::testing::Values(1, 5, 20, 50), );

class ThreadBatchProcessorTestWithBatchSize
    : public ::testing::TestWithParam<int> {};

TEST_P(ThreadBatchProcessorTestWithBatchSize, SimpleIO) {
  ThreadBatchProcessor<int> batch_processor(GetParam());
  std::mutex mu;

  std::vector<int> args(20);
  std::iota(args.begin(), args.end(), 0);

  std::set<int> out;
  batch_processor.RunInParallel(&args,
                                [&out, &mu](int* i_ptr, size_t k, size_t t) {
                                  Unused(k);
                                  Unused(t);
                                  std::lock_guard<std::mutex> lock(mu);
                                  out.insert(*i_ptr);
                                });

  std::set<int> model(args.begin(), args.end());
  ASSERT_EQ(model, out);
}

INSTANTIATE_TEST_CASE_P(SimpleThreadRunner,
                        ThreadBatchProcessorTestWithBatchSize,
                        ::testing::Values(1, 5, 20, 50), );

}  // namespace
}  // namespace nc
