#ifndef NCODE_EVENT_QUEUE_H
#define NCODE_EVENT_QUEUE_H

#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <queue>
#include <ratio>
#include <vector>

#include "common.h"
#include "logging.h"

namespace nc {

// A wrapper around an uint64_t that the event queue will use as time.
class EventQueueTime {
 public:
  static constexpr EventQueueTime ZeroTime() { return EventQueueTime(0); }

  static constexpr EventQueueTime MaxTime() {
    return EventQueueTime(std::numeric_limits<uint64_t>::max());
  }

  EventQueueTime() : m_val(0) {}

  // Explicit constructor:
  explicit constexpr EventQueueTime(uint64_t val) : m_val(val) {}

  bool isZero() const { return m_val == 0; }

  uint64_t Raw() const { return m_val; }

  EventQueueTime operator+=(EventQueueTime other) {
    return EventQueueTime(m_val += other.m_val);
  }

  friend bool operator==(EventQueueTime a, EventQueueTime b) {
    return a.m_val == b.m_val;
  }

  friend bool operator!=(EventQueueTime a, EventQueueTime b) {
    return a.m_val != b.m_val;
  }

  friend bool operator>(EventQueueTime a, EventQueueTime b) {
    return a.m_val > b.m_val;
  }

  friend bool operator>=(EventQueueTime a, EventQueueTime b) {
    return a.m_val >= b.m_val;
  }

  friend bool operator<(EventQueueTime a, EventQueueTime b) {
    return a.m_val < b.m_val;
  }

  friend bool operator<=(EventQueueTime a, EventQueueTime b) {
    return a.m_val <= b.m_val;
  }

  friend EventQueueTime operator-(EventQueueTime a, EventQueueTime b) {
    CHECK(b.m_val <= a.m_val) << "Negative time";
    return EventQueueTime(a.m_val - b.m_val);
  }

  friend EventQueueTime operator+(EventQueueTime a, EventQueueTime b) {
    return EventQueueTime(a.m_val + b.m_val);
  }

  friend EventQueueTime operator*(EventQueueTime a, size_t i) {
    return EventQueueTime(a.m_val * i);
  }

  friend EventQueueTime operator/(EventQueueTime a, double i) {
    return EventQueueTime(a.m_val / i);
  }

  friend double operator/(EventQueueTime a, EventQueueTime b) {
    return a.m_val / static_cast<double>(b.m_val);
  }

 private:
  uint64_t m_val;
};

class EventQueue;

// An entity that knows how to process events.
class EventConsumer {
 public:
  virtual ~EventConsumer();

  // The event queue.
  EventQueue* event_queue() { return parent_event_queue_; }
  const EventQueue* event_queue() const { return parent_event_queue_; }

  // A human-readable identifier. Not necessarily unique.
  const std::string& id() const { return id_; }

  // Enqueues an event for this consumer at the given time.
  void EnqueueAt(EventQueueTime at);

  // Enqueues an event for this consumer at a given time from the current time.
  void EnqueueIn(EventQueueTime in);

  // Should be called by the event queue.
  void HandleEventPublic();

  // Number of events in the event queue for this consumer.
  size_t outstanding_event_count() { return outstanding_event_count_; }

 protected:
  EventConsumer(const std::string& id, EventQueue* event_queue)
      : id_(id),
        outstanding_event_count_(0),
        parent_event_queue_(event_queue) {}

  // Processes an event.
  virtual void HandleEvent() = 0;

 private:
  const std::string id_;

  // The number of outstanding events for this consumer. If there are
  // outstanding events when this object is freed then use-after-free will occur
  // when the event queue tries to trigger one of the outstanding events. To
  // help detecting those cases the event count is explicitly maintained.
  size_t outstanding_event_count_;

  EventQueue* parent_event_queue_;
  DISALLOW_COPY_AND_ASSIGN(EventConsumer);
};

// An event queue. Manages time and can have consumers added to it.
class EventQueue {
 public:
  virtual ~EventQueue() {}

  // Current time, in time units since some implementation-dependent epoch.
  virtual EventQueueTime CurrentTime() const = 0;

  // Convenient alternative to NanosToTime that takes a number of milliseconds
  // instead of a std::chrono type.
  EventQueueTime RawMillisToTime(uint64_t duration_millis) const;

  // Convenient alternative to TimeToNanos that returns a number of
  // milliseconds instead of a std::chrono type.
  uint64_t TimeToRawMillis(EventQueueTime duration) const;

  // Public version of NanosToTime that takes in any std::chrono type.
  template <typename T>
  EventQueueTime ToTime(T duration) {
    using namespace std::chrono;
    nanoseconds nanos = duration_cast<nanoseconds>(duration);
    return NanosToTime(nanos);
  }

  // Converts from EventQueueTime to nanoseconds. Implementation-dependent.
  virtual std::chrono::nanoseconds TimeToNanos(
      EventQueueTime duration) const = 0;

  // The time the event queue will be closed and process no more events. By
  // default this time is the maximum event queue time.
  EventQueueTime StopTime() const { return stop_time_; }

  // Stops execution.
  void Stop() { stop_time_ = CurrentTime(); }

  // A convenience method.
  template <typename T>
  void RunAndStopIn(T ms) {
    stop_time_ = EventQueueTime::MaxTime();
    StopIn(std::chrono::duration_cast<std::chrono::nanoseconds>(ms));
    Run();
  }

  // Evicts from the queue all events for a given consumer. This is slow, do not
  // call often.
  void EvictConsumer(EventConsumer* consumer);

 protected:
  EventQueue() : stop_time_(EventQueueTime::MaxTime()) {}

  // Converts from nanoseconds to EventQueueTime. Implementation-dependent.
  virtual EventQueueTime NanosToTime(
      std::chrono::nanoseconds duration) const = 0;

  // Implementation-specific way to advance the time to a given point.
  virtual void AdvanceTimeTo(EventQueueTime time) = 0;

  // Runs the event queue in the calling thread. This will block until the queue
  // is closed.
  virtual void Run();

 private:
  // Schedules an EventConsumer to get an event at some point in time.
  void Enqueue(EventQueueTime at, EventConsumer* consumer);

  // Schedules an EventConsumer to get an event as soon as possible.
  void Enqueue(EventConsumer* consumer);

  // Sets the time the queue will be closed.
  void StopIn(std::chrono::nanoseconds ms);

  // The time the next event was scheduled to execute and the event itself. If
  // the event is late the first element in the pair will be less than
  // CurrentTime().
  struct ScheduledEvent {
    ScheduledEvent(EventQueueTime at, EventConsumer* consumer)
        : at(at), consumer(consumer) {}

    EventQueueTime at;
    EventConsumer* consumer;
  };

  struct Comparator {
    bool operator()(const ScheduledEvent& lhs, const ScheduledEvent& rhs) {
      return lhs.at > rhs.at;
    }
  };

  // Returns the next pending event.
  ScheduledEvent* NextEvent();

  // Pops the most recent event.
  void PopEvent() { queue_.pop(); }

  // When to stop executing events.
  EventQueueTime stop_time_;

  // The queue itself.
  VectorPriorityQueue<ScheduledEvent, Comparator> queue_;

  friend class EventConsumer;

  DISALLOW_COPY_AND_ASSIGN(EventQueue);
};

// An event queue implementation that runs on wallclock time.
class RealTimeEventQueue : public EventQueue {
 public:
  RealTimeEventQueue() {}
  EventQueueTime CurrentTime() const override;
  EventQueueTime NanosToTime(std::chrono::nanoseconds duration) const override;
  std::chrono::nanoseconds TimeToNanos(EventQueueTime duration) const override;

 protected:
  void AdvanceTimeTo(EventQueueTime at) override;
};

// An event queue implementation that runs on simulated time.
class SimTimeEventQueue : public EventQueue {
 public:
  typedef std::ratio<1l, 1000000000000l> pico;
  typedef std::chrono::duration<uint64_t, pico> picoseconds;

  SimTimeEventQueue() : time_(0) {}
  EventQueueTime CurrentTime() const override { return time_; }
  EventQueueTime NanosToTime(std::chrono::nanoseconds duration) const override;
  std::chrono::nanoseconds TimeToNanos(EventQueueTime duration) const override;

  void Run() override {
    EventQueue::Run();
    time_ = StopTime();
  }

 protected:
  void AdvanceTimeTo(EventQueueTime at) override { time_ = at; }

 private:
  EventQueueTime time_;
};

}  // namespace nc
#endif
