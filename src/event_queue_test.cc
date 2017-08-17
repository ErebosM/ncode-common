#include "event_queue.h"

#include <stddef.h>
#include <cassert>
#include <functional>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

namespace nc {
namespace {
using namespace std::chrono;

static constexpr char kDummyId[] = "Dummy";

// A simple consumer that calls a callback when the event triggers.
class DummyConsumer : public EventConsumer {
 public:
  DummyConsumer(EventQueue* event_queue, std::function<void()> callback,
                nanoseconds period = nanoseconds(0))
      : EventConsumer(kDummyId, event_queue),
        period_(event_queue->ToTime(period)),
        callback_(callback) {}

  void HandleEvent() override {
    if (!period_.isZero()) {
      EnqueueIn(period_);
    }
    callback_();
  }

 private:
  EventQueueTime period_;
  std::function<void()> callback_;
};

class EventFixture : public ::testing::Test {
 protected:
  EventFixture() {
    c1_ = make_unique<DummyConsumer>(&queue_, [this] { ints_.push_back(5); });
    c2_ = make_unique<DummyConsumer>(&queue_, [this] { ints_.push_back(3); });
    c3_ = make_unique<DummyConsumer>(&queue_, [this] { ints_.push_back(2); });
    c4_ = make_unique<DummyConsumer>(&queue_, [this] { ints_.push_back(4); });
    c5_ = make_unique<DummyConsumer>(&queue_, [this] { ints_.push_back(1); });
  }

  void AddEvents() {
    // This should trigger last, as the queue is real-time and all other events
    // are in the past.
    c1_->EnqueueIn(EventQueueTime::ZeroTime());
    c2_->EnqueueAt(queue_.ToTime(milliseconds(10)));
    c3_->EnqueueAt(queue_.ToTime(milliseconds(5)));
    c4_->EnqueueAt(queue_.ToTime(milliseconds(50)));
    c5_->EnqueueAt(queue_.ToTime(milliseconds(1)));
  }

  std::unique_ptr<DummyConsumer> c1_;
  std::unique_ptr<DummyConsumer> c2_;
  std::unique_ptr<DummyConsumer> c3_;
  std::unique_ptr<DummyConsumer> c4_;
  std::unique_ptr<DummyConsumer> c5_;

  BusyWaitEventQueue queue_;
  std::vector<int> ints_;
};

TEST_F(EventFixture, Empty) {
  queue_.RunAndStopIn(seconds(100));
  ASSERT_EQ(0ul, ints_.size());
}

TEST_F(EventFixture, Order) {
  AddEvents();

  queue_.RunAndStopIn(seconds(100));
  ASSERT_EQ(ints_, std::vector<int>({1, 2, 3, 4, 5}));
}

TEST_F(EventFixture, OrderDouble) {
  AddEvents();
  AddEvents();

  queue_.RunAndStopIn(seconds(100));
  ASSERT_EQ(ints_, std::vector<int>({1, 1, 2, 2, 3, 3, 4, 4, 5, 5}));
}

class PeriodicRunnerFixture : public ::testing::Test {
 protected:
  void AddPeriodicConsumer(nanoseconds period, std::function<void()> callback) {
    assert(!consumer_);
    consumer_ = make_unique<DummyConsumer>(&queue_, callback, period);
    consumer_->EnqueueIn(EventQueueTime::ZeroTime());
  }

  void RunFor(milliseconds duration) { queue_.RunAndStopIn(duration); }

  BusyWaitEventQueue queue_;
  std::unique_ptr<DummyConsumer> consumer_;
};

// Running with zero period should be valid.
TEST_F(PeriodicRunnerFixture, ZeroPeriod) {
  int i = 0;
  AddPeriodicConsumer(nanoseconds(1), [&i] { i++; });

  RunFor(milliseconds(100));
  ASSERT_LT(0, i);
}

TEST_F(PeriodicRunnerFixture, AvgPeriod) {
  int i = 0;
  AddPeriodicConsumer(nanoseconds(milliseconds(10)), [&i] { i++; });

  RunFor(milliseconds(500));
  ASSERT_NEAR(50, i, 5);
}

TEST_F(PeriodicRunnerFixture, SlowTask) {
  int i = 0;
  AddPeriodicConsumer(nanoseconds(milliseconds(6)), [&i] {
    std::this_thread::sleep_for(milliseconds(5));
    i++;
  });

  RunFor(milliseconds(600));
  ASSERT_NEAR(100, i, 5);
}

TEST_F(PeriodicRunnerFixture, VerySlowTaskComplete) {
  int i = 0;
  AddPeriodicConsumer(nanoseconds(milliseconds(10)), [&i] {
    std::this_thread::sleep_for(milliseconds(500));
    i++;
  });

  RunFor(milliseconds(100));

  // The slow task should complete, there should only be one invocation of the
  // task.
  ASSERT_EQ(1, i);
}

// The destructor of the task object should cleanly terminate the task and join
// the running thread.
TEST_F(PeriodicRunnerFixture, CleanDestruction) {
  AddPeriodicConsumer(nanoseconds(milliseconds(10)),
                      [] { std::this_thread::sleep_for(milliseconds(1)); });
}

TEST_F(PeriodicRunnerFixture, AverageRate) {
  int i = 0;
  AddPeriodicConsumer(nanoseconds(milliseconds(20)), [&i] {
    size_t sleep_time = 10;
    i++;
    if (i == 5) {
      sleep_time = 100;
    }

    std::this_thread::sleep_for(milliseconds(sleep_time));
  });
  RunFor(milliseconds(4000));

  // When one of the calls sleeps an order of magnitude more the subsequent
  // calls should be sped up, so that the total number of calls is as if the
  // slow call was a regular one.
  ASSERT_NEAR(200, i, 5);
}

class SimEventQueueFixture : public ::testing::Test {
 protected:
  SimTimeEventQueue queue_;
};

TEST_F(SimEventQueueFixture, Init) {
  ASSERT_EQ(EventQueueTime::ZeroTime(), queue_.CurrentTime());
  ASSERT_EQ(EventQueueTime::MaxTime(), queue_.StopTime());
}

TEST_F(SimEventQueueFixture, RunUntil) {
  queue_.RunAndStopIn(milliseconds(100));
  ASSERT_EQ(queue_.TimeToNanos(queue_.CurrentTime()), milliseconds(100));
}

TEST_F(SimEventQueueFixture, ScheduleAt) {
  bool tmp = false;
  DummyConsumer consumer(&queue_, [&tmp] { tmp = true; });
  consumer.EnqueueAt(queue_.ToTime(milliseconds(500)));
  queue_.RunAndStopIn(milliseconds(1000));

  ASSERT_TRUE(tmp);
  ASSERT_EQ(queue_.TimeToNanos(queue_.CurrentTime()), milliseconds(1000));
}

TEST_F(SimEventQueueFixture, ScheduleAtTooShort) {
  bool tmp = false;
  DummyConsumer consumer(&queue_, [&tmp] { tmp = true; });
  consumer.EnqueueAt(queue_.ToTime(milliseconds(500)));
  queue_.RunAndStopIn(milliseconds(100));
  ASSERT_FALSE(tmp);
}

TEST_F(SimEventQueueFixture, ScheduleAtExact) {
  bool tmp = false;
  DummyConsumer consumer(&queue_, [&tmp] { tmp = true; });
  consumer.EnqueueAt(queue_.ToTime(milliseconds(500)));
  queue_.RunAndStopIn(milliseconds(500));

  // The kill event should take precedence over anything else in the queue.
  ASSERT_FALSE(tmp);
}

TEST_F(SimEventQueueFixture, RunTwice) {
  bool tmp = false;
  DummyConsumer consumer(&queue_, [&tmp] { tmp = true; });
  queue_.RunAndStopIn(milliseconds(500));
  consumer.EnqueueAt(queue_.ToTime(milliseconds(500)));
  consumer.EnqueueIn(EventQueueTime::ZeroTime());
  consumer.EnqueueAt(queue_.ToTime(milliseconds(500)));
  queue_.RunAndStopIn(milliseconds(5000));
  ASSERT_TRUE(tmp);
}

TEST_F(SimEventQueueFixture, RawMillis) {
  uint64_t millis_at = 0;
  EventQueueTime time_at;

  DummyConsumer consumer(&queue_, [this, &millis_at, &time_at] {
    time_at = queue_.CurrentTime();
    millis_at = queue_.TimeToRawMillis(queue_.CurrentTime());
  });
  consumer.EnqueueAt(queue_.ToTime(milliseconds(500)));
  queue_.RunAndStopIn(milliseconds(1000));
  ASSERT_EQ(500ul, millis_at);
  ASSERT_EQ(queue_.RawMillisToTime(500), time_at);
}

}  // namespace
}  // namespace nc
