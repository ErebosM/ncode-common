#ifndef FLOW_COUNTER_PCAP_DATA_H
#define FLOW_COUNTER_PCAP_DATA_H

#include <map>

#include "../event_queue.h"
#include "../net/net_common.h"

namespace nc {
namespace htsim {

class FlowCounter {
 public:
  FlowCounter(EventQueue* event_queue)
      : period_start_(event_queue->CurrentTime()), event_queue_(event_queue) {}

  // Adds a new packet. This assumes that now is
  void NewPacket(const net::FiveTuple& five_tuple);

  // Returns an estimate for the expected number of flows that the counter has
  // seen since the last call to EstimateCount. Same as calling
  // EstaimteCountConst() and Clear().
  double EstimateCount();

  // A version of EstimateCount that does not automatically clear current
  // flows. Should call Clear() later.
  double EstimateCountConst() const;

  void Clear() { flows_.clear(); }

  EventQueue* event_queue() { return event_queue_; }

 private:
  struct FirstAndLast {
    FirstAndLast(EventQueueTime first) : first(first), last(first) {}

    EventQueueTime first;
    EventQueueTime last;
  };

  // Time the last call to EstimateCount occurred (or object construction).
  EventQueueTime period_start_;

  // Records the timestamps of first and last packets seen from flows.
  std::map<net::FiveTuple, FirstAndLast> flows_;

  // The event queue -- used for getting timestamps.
  EventQueue* event_queue_;
};

}  // namespace htsim
}  // namespace nc

#endif
