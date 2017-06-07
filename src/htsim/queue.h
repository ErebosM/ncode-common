#ifndef NCODE_HTSIM_QUEUE_H
#define NCODE_HTSIM_QUEUE_H

#include <stddef.h>
#include <cstdint>
#include <deque>
#include <random>
#include <string>
#include <utility>

#include "../common.h"
#include "../event_queue.h"
#include "../stats.h"
#include "../net/net_common.h"
#include "packet.h"
#include "animator.h"

namespace nc {
namespace htsim {

// Stats about a Pipe (below).
struct PipeStats {
  uint64_t pkts_tx = 0;
  uint64_t bytes_tx = 0;
  uint64_t pkts_in_flight = 0;
  uint64_t bytes_in_flight = 0;
};

// A pipe adds some constant delay to all incoming packets
class Pipe : public EventConsumer, public PacketHandler {
 public:
  Pipe(const net::GraphLink& graph_link, EventQueue* event_queue);
  Pipe(const std::string& src, const std::string& dst, EventQueueTime delay,
       EventQueue* event_queue);

  // Connects this pipe to a handler. All packets will be processed by this
  // handler as they exit the pipe.
  void Connect(PacketHandler* handler) { other_end_ = handler; }

  void HandleEvent() override;

  void HandlePacket(PacketPtr pkt) override;

  const PipeStats& GetStats() const { return stats_; }

  const net::GraphLink* graph_link() const {
    CHECK(graph_link_ != nullptr);
    return graph_link_;
  }

 private:
  typedef std::pair<EventQueueTime, PacketPtr> ServiceTimeAndPacket;

  // The amount of delay to add.
  const EventQueueTime delay_;

  // The entity that will handle packets when they are dequeued.
  PacketHandler* other_end_;

  // The packets in flight.
  std::deque<ServiceTimeAndPacket> queue_;

  // The link this pipe is associated with.
  const net::GraphLink* graph_link_;

  PipeStats stats_;

  DISALLOW_COPY_AND_ASSIGN(Pipe);
};

// Stats about a queue (below).
struct QueueStats {
  uint64_t queue_size_bytes = 0;
  uint64_t queue_size_pkts = 0;
  uint64_t pkts_seen = 0;
  uint64_t pkts_dropped = 0;
  uint64_t bytes_seen = 0;
  uint64_t bytes_dropped = 0;
};

// A common interface for all queues.
class Queue : public EventConsumer,
              public PacketHandler,
              public AnimationComponent {
 public:
  // Connects this queue to a handler. All packets will be processed by this
  // handler as they are dequeued.
  void Connect(PacketHandler* handler) { other_end_ = handler; }

  // Returns the status of this queue. It is up to subclasses to update the
  // status object.
  const QueueStats& GetStats() const { return stats_; }

  // Gets the current rate.
  virtual net::Bandwidth GetRate() const = 0;

  // Changes the drain rate of this queue.
  virtual void SetRate(net::Bandwidth rate) = 0;

  // Applying the value as an animated component means changing the rate.
  void ApplyValue(double value) override;

 protected:
  Queue(const net::GraphLink& edge, EventQueue* event_queue);
  Queue(const std::string& src, const std::string& dst,
        EventQueue* event_queue);

  // The entity that will handle packets when they are dequeued.
  PacketHandler* other_end_;

  // Status of this queue.
  QueueStats stats_;

  // Number of bits transmitted over the last period.
  size_t bits_seen_in_last_period_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Queue);
};

// A simple FIFO queue.
class FIFOQueue : public Queue {
 public:
  FIFOQueue(const net::GraphLink& edge, uint64_t max_size_bytes,
            EventQueue* event_queue);
  FIFOQueue(const std::string& src, const std::string& dst, net::Bandwidth rate,
            uint64_t max_size_bytes, EventQueue* event_queue);

  virtual bool ShouldDrop(uint64_t pkt_size_bytes) {
    return stats_.queue_size_bytes + pkt_size_bytes > max_size_bytes_;
  }

  void HandlePacket(PacketPtr pkt) override;

  void HandleEvent() override;

  void SetRate(net::Bandwidth rate) override;

  net::Bandwidth GetRate() const override { return rate_; }

 protected:
  inline EventQueueTime PacketDrainTime(const Packet& pkt) {
    return EventQueueTime(time_per_bit_.Raw() * pkt.size_bytes() * 8);
  }

  // Queue capacity.
  const uint64_t max_size_bytes_;

  // The drain rate of the queue.
  net::Bandwidth rate_;

  // Time to process a single bit.
  EventQueueTime time_per_bit_;

  // Enqueued packets.
  std::deque<PacketPtr> queue_;

  // Keeps track of the amounts of time packets are waiting.
  SummaryStats time_waiting_;

  DISALLOW_COPY_AND_ASSIGN(FIFOQueue);
};

// A link is a combination of a pipe and a queue.
struct Link {
  std::unique_ptr<Pipe> pipe;
  std::unique_ptr<Queue> queue;
};

// A queue that randomly drops packets above a threshold.
class RandomQueue : public FIFOQueue {
 public:
  RandomQueue(const net::GraphLink& edge, uint64_t max_size_bytes,
              uint64_t drop_threshold_bytes, double seed,
              EventQueue* event_queue);
  RandomQueue(const std::string& src, const std::string& dst,
              net::Bandwidth rate, uint64_t max_size_bytes,
              uint64_t drop_threshold_bytes, double seed,
              EventQueue* event_queue);

  bool ShouldDrop(uint64_t pkt_size_bytes) override;

 private:
  // Dropping starts above this value
  const uint64_t drop_threshold_bytes_;

  // The random number generator
  std::mt19937 rnd_;

  // The distribution, the range is 0-1 by default.
  std::uniform_real_distribution<double> dis_;

  DISALLOW_COPY_AND_ASSIGN(RandomQueue);
};

}  // namespace htsim
}  // namespace ncode

#endif
