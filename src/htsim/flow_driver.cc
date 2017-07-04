#include "flow_driver.h"

#include <algorithm>
#include <limits>

#include "../logging.h"
#include "packet.h"

namespace nc {
namespace htsim {

void ManualFlowDriver::AddData(const std::vector<AddDataEvent>& events) {
  add_data_events_.insert(add_data_events_.end(), events.begin(), events.end());
  std::stable_sort(add_data_events_.begin(), add_data_events_.end(),
                   [](const AddDataEvent& lhs, const AddDataEvent& rhs) {
                     return lhs.at > rhs.at;
                   });
}

AddDataEvent ManualFlowDriver::Next() {
  AddDataEvent to_return = PeekNextAddData();
  PopNextAddData();
  return to_return;
}

const AddDataEvent& ManualFlowDriver::PeekNextAddData() {
  if (add_data_events_.empty()) {
    return kAddDataInfinity;
  }

  return add_data_events_.back();
}

void ManualFlowDriver::PopNextAddData() {
  if (!add_data_events_.empty()) {
    add_data_events_.pop_back();
  }
}

RateKeyFrame ConstantRateFlowDriver::NextKeyFrame() {
  if (next_key_frame_index_ == rate_change_key_frames_.size()) {
    return RateKeyFrame(EventQueueTime::MaxTime(), curr_rate_);
  }
  return rate_change_key_frames_[next_key_frame_index_];
}

void ConstantRateFlowDriver::AdvanceToNextKeyFrame() {
  RateKeyFrame next_key_frame = NextKeyFrame();
  curr_time_ = next_key_frame.at;
  curr_rate_ = next_key_frame.rate_bps;
  inter_packet_gap_ =
      EventQueueTime(second_.Raw() / ((curr_rate_ / 8.0) / packet_size_bytes_));
  ++next_key_frame_index_;
}

AddDataEvent ConstantRateFlowDriver::Next() {
  if (curr_time_ == EventQueueTime::MaxTime()) {
    return {EventQueueTime::MaxTime(), 0};
  }

  EventQueueTime next_packet_time = curr_time_ + inter_packet_gap_;
  RateKeyFrame next_key_frame = NextKeyFrame();
  if (inter_packet_gap_.isZero() || next_key_frame.at < next_packet_time) {
    AdvanceToNextKeyFrame();
    return Next();
  }
  curr_time_ = next_packet_time;
  return {next_packet_time, packet_size_bytes_};
}

void ConstantRateFlowDriver::AddRateChangeKeyframes(
    const std::vector<RateKeyFrame>& key_frames) {
  rate_change_key_frames_.insert(rate_change_key_frames_.end(),
                                 key_frames.begin(), key_frames.end());
  std::stable_sort(rate_change_key_frames_.begin(),
                   rate_change_key_frames_.end(),
                   [](const RateKeyFrame& lhs, const RateKeyFrame& rhs) {
                     return lhs.at < rhs.at;
                   });
}

FeedbackLoopFlowDriver::FeedbackLoopFlowDriver(
    const std::string& id,
    std::unique_ptr<ObjectSizeAndWaitTimeGenerator> generator,
    EventQueue* event_queue)
    : EventConsumer(id, event_queue),
      generator_(std::move(generator)),
      data_to_add_(0),
      connection_(nullptr) {
  ScheduleNext();
}

void FeedbackLoopFlowDriver::HandleEvent() {
  uint64_t prev_data_to_add = data_to_add_;
  if (!data_to_add_) {
    ScheduleNext();
  }

  if (prev_data_to_add) {
    connection_->OnSendBufferDrained([this] { ScheduleNext(); });
    connection_->AddData(data_to_add_);
  }
}

void FeedbackLoopFlowDriver::ConnectionAttached(Connection* connection) {
  connection_ = connection;
}

void FeedbackLoopFlowDriver::ScheduleNext() {
  ObjectSizeAndWaitTime next = generator_->Next();
  data_to_add_ = next.object_size;
  EnqueueIn(next.wait_time);
}

OpenLoopFlowDriver::OpenLoopFlowDriver(
    const std::string& id,
    std::unique_ptr<ObjectSizeAndWaitTimeGenerator> generator,
    EventQueue* event_queue)
    : EventConsumer(id, event_queue),
      generator_(std::move(generator)),
      data_to_add_(0),
      connection_(nullptr) {
  ScheduleNext();
}

void OpenLoopFlowDriver::HandleEvent() {
  connection_->OnSendBufferDrained([] {});
  connection_->AddData(data_to_add_);
  ScheduleNext();
}

void OpenLoopFlowDriver::ConnectionAttached(Connection* connection) {
  connection_ = connection;
}

void OpenLoopFlowDriver::ScheduleNext() {
  ObjectSizeAndWaitTime next = generator_->Next();
  data_to_add_ = next.object_size;
  EnqueueIn(next.wait_time);
}

void FlowPack::Init() {
  AddFirstEvents();
  num_events_cached_ = CacheEvents();

  if (num_events_cached_) {
    EnqueueAt(pending_events_[0].add_data_event.at);
  }
}

void FlowPack::AddDriver(std::unique_ptr<FlowDriver> driver,
                         Connection* connection) {
  if (driver->type() == FlowDriver::INDEPENDENT) {
    std::unique_ptr<IndependentFlowDriver> independent_flow_driver(
        static_cast<IndependentFlowDriver*>(driver.release()));
    ConnectionAndIndependentDriver connection_and_driver;
    connection_and_driver.driver = std::move(independent_flow_driver);
    connection_and_driver.connection = connection;
    independent_drivers_.emplace_back(std::move(connection_and_driver));
  } else if (driver->type() == FlowDriver::DEPENDENT) {
    std::unique_ptr<ConnectionDependentFlowDriver> dependent_flow_driver(
        static_cast<ConnectionDependentFlowDriver*>(driver.release()));
    dependent_flow_driver->ConnectionAttached(connection);
    dependent_drivers_.emplace_back(std::move(dependent_flow_driver));
  }
}

void FlowPack::HandleEvent() {
  const Event& ev = pending_events_[next_event_index_++];
  Connection* connection = ev.connection_and_driver->connection;
  if (ev.add_data_event.close) {
    connection->Close();
  } else {
    uint64_t data_to_add = ev.add_data_event.bytes;
    if (data_to_add) {
      connection->AddData(data_to_add);
    }
  }

  if (next_event_index_ == num_events_cached_) {
    num_events_cached_ = CacheEvents();
    next_event_index_ = 0;

    if (num_events_cached_ == 0) {
      return;
    }
  }

  EnqueueAt(pending_events_[next_event_index_].add_data_event.at);
}

void FlowPack::AddFirstEvents() {
  Event ev;
  for (auto& connection_and_driver : independent_drivers_) {
    ev.connection_and_driver = &connection_and_driver;
    ev.add_data_event = connection_and_driver.driver->Next();
    if (ev.add_data_event.at != EventQueueTime::MaxTime()) {
      queue_.emplace(std::move(ev));
    }
  }
}

size_t FlowPack::CacheEvents() {
  size_t i;

  Event ev;
  for (i = 0; i < kEventCacheSize; ++i) {
    if (!queue_.size()) {
      break;
    }

    pending_events_[i] = std::move(const_cast<Event&>(queue_.top()));
    queue_.pop();

    Event& curr_event = pending_events_[i];
    ev.connection_and_driver = curr_event.connection_and_driver;
    ev.add_data_event = curr_event.connection_and_driver->driver->Next();
    if (ev.add_data_event.at != EventQueueTime::MaxTime()) {
      queue_.emplace(std::move(ev));
    }
  }

  LOG(INFO) << "Cached " << i << " events";
  return i;
}

DefaultObjectSizeAndWaitTimeGenerator::DefaultObjectSizeAndWaitTimeGenerator(
    size_t mean_object_size_bytes, bool size_fixed,
    std::chrono::milliseconds mean_wait_time_ms, bool wait_time_fixed,
    double seed, EventQueue* event_queue)
    : mean_object_size_(mean_object_size_bytes),
      mean_wait_time_(mean_wait_time_ms.count()),
      object_size_fixed_(size_fixed),
      wait_time_fixed_(wait_time_fixed),
      generator_(seed),
      object_size_distribution_(1.0 / mean_object_size_bytes),
      wait_time_distribution_(1.0 / mean_wait_time_ms.count()),
      constant_delay_ms_(0),
      event_queue_(event_queue) {}

ObjectSizeAndWaitTime DefaultObjectSizeAndWaitTimeGenerator::Next() {
  bool max_object_size =
      mean_object_size_ == std::numeric_limits<uint64_t>::max();
  bool min_wait_time = mean_wait_time_ == 0;

  size_t object_size_bytes;
  if (max_object_size) {
    object_size_bytes = std::numeric_limits<uint64_t>::max();
  } else if (object_size_fixed_) {
    object_size_bytes = mean_object_size_;
  } else {
    object_size_bytes = object_size_distribution_(generator_);
    object_size_bytes = std::max(1ul, object_size_bytes);
  }

  size_t wait_time_ms;
  if (min_wait_time) {
    wait_time_ms = 0;
  } else if (wait_time_fixed_) {
    wait_time_ms = mean_wait_time_;
  } else {
    wait_time_ms = constant_delay_ms_ + wait_time_distribution_(generator_);
    wait_time_ms = std::max(1ul, wait_time_ms);
  }

  return {object_size_bytes, event_queue_->RawMillisToTime(wait_time_ms)};
}

}  // namespace htsim
}  // namespace ncode
