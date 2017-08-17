#include "event_queue.h"

#include <thread>

namespace nc {
using namespace std::chrono;

EventConsumer::~EventConsumer() {
  if (outstanding_event_count_ > 0) {
    LOG(INFO)
        << "Tried to destroy EventCounsumer (" << id() << ") with "
        << outstanding_event_count_
        << " outstanding events. Will evict the consumer from the queue. Fix "
           "your code if this happens a lot.";
    parent_event_queue_->EvictConsumer(this);
  }
}

void EventConsumer::EnqueueAt(EventQueueTime at) {
  ++outstanding_event_count_;
  parent_event_queue_->Enqueue(at, this);
}

void EventConsumer::EnqueueIn(EventQueueTime in) {
  ++outstanding_event_count_;
  parent_event_queue_->Enqueue(parent_event_queue_->CurrentTime() + in, this);
}

void EventConsumer::HandleEventPublic() {
  --outstanding_event_count_;
  HandleEvent();
}

void EventQueue::Run() {
  while (!queue_.empty()) {
    ScheduledEvent* next_event = NextEvent();
    EventQueueTime now = CurrentTime();
    if (now >= stop_time_) {
      break;
    }

    EventConsumer* consumer = next_event->consumer;
    PopEvent();
    consumer->HandleEventPublic();
  }
}

void EventQueue::StopIn(std::chrono::nanoseconds ms) {
  EventQueueTime delta = NanosToTime(ms);
  auto new_kill_time = CurrentTime() + delta;
  if (new_kill_time < stop_time_) {
    stop_time_ = new_kill_time;
  }
}

void EventQueue::Enqueue(EventQueueTime at, EventConsumer* consumer) {
  queue_.emplace(at, consumer);
}

void EventQueue::Enqueue(EventConsumer* consumer) {
  Enqueue(CurrentTime(), consumer);
}

EventQueueTime EventQueue::RawMillisToTime(uint64_t duration_millis) const {
  return NanosToTime(nanoseconds(milliseconds(duration_millis)));
}

uint64_t EventQueue::TimeToRawMillis(EventQueueTime duration) const {
  nanoseconds nanos = TimeToNanos(duration);
  return duration_cast<milliseconds>(nanos).count();
}

EventQueue::ScheduledEvent* EventQueue::NextEvent() {
  // Very ugly, but should be fine, since we will pop the element immediately.
  ScheduledEvent* next_event = const_cast<ScheduledEvent*>(&(queue_.top()));
  AdvanceTimeTo(next_event->at);
  return next_event;
}

void EventQueue::EvictConsumer(EventConsumer* consumer) {
  std::vector<ScheduledEvent> events;
  while (!queue_.empty()) {
    events.emplace_back(queue_.PopTop());
  }

  for (const ScheduledEvent& event : events) {
    if (event.consumer != consumer) {
      queue_.emplace(event.at, event.consumer);
    }
  }
}

static EventQueueTime CurrentRealTimeFromNanos() {
  using namespace std::chrono;

  auto duration = high_resolution_clock::now().time_since_epoch();
  nanoseconds duration_nanos = duration_cast<nanoseconds>(duration);
  return EventQueueTime(duration_nanos.count());
}

// Assembly code to read the TSC
static inline uint64_t RDTSC() {
  unsigned int hi, lo;
  __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)hi << 32) | lo;
}

static double CalibrateTicks() {
  using namespace std::chrono;

  auto t1 = high_resolution_clock::now();
  uint64_t begin = RDTSC();
  std::this_thread::sleep_for(seconds(5));
  uint64_t end = RDTSC();
  auto t2 = high_resolution_clock::now();
  nanoseconds duration = duration_cast<nanoseconds>(t2 - t1);
  return static_cast<double>(end - begin) / duration.count();
}

EventQueueTime RealTimeEventQueue::CurrentTime() const {
  return CurrentRealTimeFromNanos();
}

EventQueueTime RealTimeEventQueue::NanosToTime(
    std::chrono::nanoseconds duration) const {
  return EventQueueTime(duration.count());
}

std::chrono::nanoseconds RealTimeEventQueue::TimeToNanos(
    EventQueueTime duration) const {
  return std::chrono::nanoseconds(duration.Raw());
}

void RealTimeEventQueue::AdvanceTimeTo(EventQueueTime at) {
  auto current_time = CurrentTime();
  if (current_time >= at) {
    return;
  }

  auto sleep_for = at - current_time;
  std::this_thread::sleep_for(TimeToNanos(sleep_for));
}

EventQueueTime SimTimeEventQueue::NanosToTime(
    std::chrono::nanoseconds duration) const {
  return EventQueueTime(
      std::chrono::duration_cast<picoseconds>(duration).count());
}

std::chrono::nanoseconds SimTimeEventQueue::TimeToNanos(
    EventQueueTime duration) const {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      picoseconds(duration.Raw()));
}

BusyWaitEventQueue::BusyWaitEventQueue() : ticks_per_nano_(CalibrateTicks()) {}

std::chrono::nanoseconds BusyWaitEventQueue::TimeToNanos(
    EventQueueTime duration) const {
  return std::chrono::nanoseconds(duration.Raw());
}

EventQueueTime BusyWaitEventQueue::NanosToTime(
    std::chrono::nanoseconds duration) const {
  return EventQueueTime(duration.count());
}

EventQueueTime BusyWaitEventQueue::CurrentTime() const {
  uint64_t counter = RDTSC();
  double nanos = counter / ticks_per_nano_;
  return EventQueueTime(nanos);
}

void BusyWaitEventQueue::AdvanceTimeTo(EventQueueTime at) {
  while (true) {
    EventQueueTime current_time = CurrentTime();
    if (current_time >= at) {
      break;
    }
  }
}

}  // namespace nc
