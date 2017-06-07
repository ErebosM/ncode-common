#ifndef NCODE_HTSIM_FLOW_DRIVER_H
#define NCODE_HTSIM_FLOW_DRIVER_H

#include <stddef.h>
#include <array>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <queue>
#include <random>
#include <vector>

#include "../common.h"
#include "../event_queue.h"

namespace nc {
namespace htsim {
class Connection;
} /* namespace htsim */
} /* namespace nc */

namespace nc {
namespace htsim {
class Device;
} /* namespace htsim */
} /* namespace nc */

namespace nc {
namespace htsim {

// A single key frame in the lifetime of fixed-rate flow. Shows what the rate of
// the flow is at a given point in time.
struct RateKeyFrame {
  constexpr RateKeyFrame(EventQueueTime at, uint64_t rate_bps)
      : at(at), rate_bps(rate_bps) {}

  friend bool operator==(const RateKeyFrame& a, const RateKeyFrame& b) {
    return a.at == b.at && a.rate_bps == b.rate_bps;
  }

  EventQueueTime at;
  uint64_t rate_bps;
};

// Adds data to the sending buffer of a connection.
struct AddDataEvent {
  constexpr AddDataEvent(EventQueueTime at, uint64_t bytes)
      : at(at), bytes(bytes), close(false) {}

  friend bool operator==(const AddDataEvent& a, const AddDataEvent& b) {
    return a.at == b.at && a.bytes == b.bytes;
  }

  EventQueueTime at;
  uint64_t bytes;
  bool close;
};

static constexpr AddDataEvent kAddDataInfinity =
    AddDataEvent(EventQueueTime::MaxTime(), 0);

// An entity that will add data to a connection.
class FlowDriver {
 public:
  enum Type {
    // Independent flow drivers can have their connection's events precomputed
    // ahead of time. The addition of data to connection only depends on the
    // driver itself.
    INDEPENDENT,

    // Dependent flow drivers add data based on some conditions external to the
    // driver.
    DEPENDENT
  };

  FlowDriver(Type type) : type_(type) {}

  // Returns the type of the event.
  Type type() { return type_; }

 private:
  const Type type_;

  DISALLOW_COPY_AND_ASSIGN(FlowDriver);
};

// An entity that will generate AddDataEvents for a connection.
class IndependentFlowDriver : public FlowDriver {
 public:
  IndependentFlowDriver() : FlowDriver(FlowDriver::INDEPENDENT) {}

  virtual ~IndependentFlowDriver() {}

  // Returns the next AddDataEvent for the flow managed by the driver. This will
  // remove the event from the internal datastructures.
  virtual AddDataEvent Next() = 0;
};

// A flow driver that only knows how to operate in the context of a connection.
class ConnectionDependentFlowDriver : public FlowDriver {
 public:
  ConnectionDependentFlowDriver() : FlowDriver(FlowDriver::DEPENDENT) {}

  virtual ~ConnectionDependentFlowDriver() {}

  // Called when the driver is connected to a connection.
  virtual void ConnectionAttached(Connection* connection) = 0;
};

// A driver that produces AddDataEvents at a fixed rate. The rate can be changed
// by adding KeyFrames. The rate is varied by varying the time between sending
// fixed-amount data packets.
class ConstantRateFlowDriver : public IndependentFlowDriver {
 public:
  ConstantRateFlowDriver(uint32_t packet_size_bytes, EventQueueTime second)
      : packet_size_bytes_(packet_size_bytes),
        next_key_frame_index_(0),
        curr_rate_(0),
        second_(second) {}

  // Finds the next AddData event for this connection.
  AddDataEvent Next() override;

  // Adds keyframes to this UDP flow's timeline.
  void AddRateChangeKeyframes(const std::vector<RateKeyFrame>& keyframes);

 private:
  // Returns the next key frame.
  RateKeyFrame NextKeyFrame();

  // Advances time to the key frame returned by NextKeyFrame.
  void AdvanceToNextKeyFrame();

  // How big each UDP packet should be.
  const uint32_t packet_size_bytes_;

  // The inter-packet gap. Depends on the current rate.
  EventQueueTime inter_packet_gap_;

  // Key frames to be used when generating AddDataEvents.
  std::vector<RateKeyFrame> rate_change_key_frames_;

  size_t next_key_frame_index_;

  // Current time.
  EventQueueTime curr_time_;

  // Current rate in bps.
  uint64_t curr_rate_;

  // When computing the inter-packet gap we need to know how much time a second
  // is in event queue time.
  EventQueueTime second_;
};

// A flow driver that will produce manually pre-configured AddData events.
class ManualFlowDriver : public IndependentFlowDriver {
 public:
  // Adds some data to the tx buffer of the connection at a given point in time.
  void AddData(const std::vector<AddDataEvent>& events);

  // Returns the next AddDataEvent.
  AddDataEvent Next();

 private:
  const AddDataEvent& PeekNextAddData();

  void PopNextAddData();

  // Events to add data to the connection.
  std::vector<AddDataEvent> add_data_events_;
};

struct ObjectSizeAndWaitTime {
  uint64_t object_size;
  EventQueueTime wait_time;
};

// A class that knows how to generate wait times and object sizes.
class ObjectSizeAndWaitTimeGenerator {
 public:
  virtual ~ObjectSizeAndWaitTimeGenerator() {}

  // Get the next ObjectSizeAndWaitTime.
  virtual ObjectSizeAndWaitTime Next() = 0;
};

// An ObjectSizeAndWaitTimeGenerator that generates exponential sizes and wait
// times. There are two special cases -- if the mean object size is set to
// uint64_t::max then an infinite amount of data will be added to the
// connection. If the mean wait time is 0, there will always be no wait time and
// data will be transmitted immediately. If size_fixed of wait_time_fixed are
// set then instead of the sizes/wait times being random they will be constant.
class DefaultObjectSizeAndWaitTimeGenerator
    : public ObjectSizeAndWaitTimeGenerator {
 public:
  DefaultObjectSizeAndWaitTimeGenerator(
      size_t mean_object_size_bytes, bool size_fixed,
      std::chrono::milliseconds mean_wait_time_ms, bool wait_time_fixed,
      double seed, EventQueue* event_queue);

  ObjectSizeAndWaitTime Next() override;

  void set_constant_delay_ms(size_t constant_delay_ms) {
    constant_delay_ms_ = constant_delay_ms;
  }

 private:
  size_t mean_object_size_;
  size_t mean_wait_time_;
  bool object_size_fixed_;
  bool wait_time_fixed_;

  std::default_random_engine generator_;
  std::exponential_distribution<double> object_size_distribution_;
  std::exponential_distribution<double> wait_time_distribution_;

  // A constant delay to be added to all wait times.
  size_t constant_delay_ms_;

  EventQueue* event_queue_;
};

// A flow driver that will add data to a connection, wait an amount of time
// after the connection's send buffer has been drained and then add more data.
class FeedbackLoopFlowDriver : public ConnectionDependentFlowDriver,
                               public EventConsumer {
 public:
  // Will get the object sizes and wait times from a vector.
  FeedbackLoopFlowDriver(
      const std::string& id,
      std::unique_ptr<ObjectSizeAndWaitTimeGenerator> generator,
      EventQueue* event_queue);

  void HandleEvent() override;

  void ConnectionAttached(Connection* connection) override;

 private:
  void ScheduleNext();

  // This object owns the generator.
  std::unique_ptr<ObjectSizeAndWaitTimeGenerator> generator_;

  // Bytes of data to add on the next HandleEvent call.
  uint64_t data_to_add_;

  // The connection that this driver adds to.
  Connection* connection_;
};

// A flow pack is a collection of connections managed by their drivers.
class FlowPack : public EventConsumer {
 public:
  static constexpr size_t kEventCacheSize = 1000000;

  FlowPack(const std::string& id, EventQueue* event_queue)
      : EventConsumer(id, event_queue),
        next_event_index_(0),
        num_events_cached_(0) {}

  // Should be called after adding all drivers and connections.
  void Init();

  // Adds a driver and a connection that should be managed by the driver.
  void AddDriver(std::unique_ptr<FlowDriver> driver, Connection* connection);

  // Looks for the next pending event. If there are no pending events will
  // generate in bulk events.
  void HandleEvent() override;

  // Goes through all flows and adds their first event to the queue.
  void AddFirstEvents();

 private:
  struct ConnectionAndIndependentDriver {
    Connection* connection;
    std::unique_ptr<IndependentFlowDriver> driver;
  };

  struct Event {
    Event()
        : add_data_event(EventQueueTime::MaxTime(), 0),
          connection_and_driver(nullptr) {}

    AddDataEvent add_data_event;
    ConnectionAndIndependentDriver* connection_and_driver;
  };

  struct Comparator {
    bool operator()(const Event& lhs, const Event& rhs) {
      return lhs.add_data_event.at > rhs.add_data_event.at;
    }
  };

  size_t CacheEvents();

  // The queue that contains events.
  std::priority_queue<Event, std::vector<Event>, Comparator> queue_;

  // A cache of pending events.
  std::array<Event, kEventCacheSize> pending_events_;

  size_t next_event_index_;

  size_t num_events_cached_;

  // All drivers.
  std::vector<ConnectionAndIndependentDriver> independent_drivers_;
  std::vector<std::unique_ptr<ConnectionDependentFlowDriver>>
      dependent_drivers_;
};

}  // namespace htsim
}  // namepsace ncode
#endif
