// Generates bulk traffic with no external congestion control loop. Similar to
// attaching a lot of UDP generators to a FlowDriver, but adds all packets to a
// single pipe.

#ifndef NCODE_HTSIM_BULK_GEN_H
#define NCODE_HTSIM_BULK_GEN_H

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "../common.h"
#include "../event_queue.h"
#include "../ptr_queue.h"
#include "../net/net_common.h"
#include "packet.h"

namespace nc {
namespace htsim {

// A source of packets that the generator uses. The packets do not need to come
// from the same five tuple.
class BulkPacketSource {
 public:
  virtual ~BulkPacketSource() {}

  // Returns the next packet. The packet will be sent according to the time_sent
  // field. This assumes that the time_sent field of packets is increasing.
  virtual PacketPtr NextPacket() = 0;
};

// Common things for all BulkPacket generators.
class BulkPacketGeneratorBase {
 public:
  BulkPacketGeneratorBase(
      std::vector<std::unique_ptr<BulkPacketSource>> sources,
      htsim::PacketHandler* out);

  void set_default_tag(PacketTag tag) { default_tag_ = tag; }

  // Sets a new output handler.
  void set_out(htsim::PacketHandler* out) {
    CHECK(out != nullptr);
    out_ = out;
  }

 protected:
  struct Event {
    Event(PacketPtr pkt, BulkPacketSource* source)
        : pkt(std::move(pkt)), source(source) {}

    PacketPtr pkt;
    BulkPacketSource* source;
  };

  struct Comparator {
    bool operator()(const Event& lhs, const Event& rhs) {
      return lhs.pkt->time_sent() > rhs.pkt->time_sent();
    }
  };

  // Adds initial events for each source.
  void AddInitialEvents();

  // Adds a new event to queue_ from a given source.
  void AddEventFromSource(BulkPacketSource* source);

  // Fetches the next packet from the queue.
  PacketPtr NextPacket();

  // Handler to output packets to.
  htsim::PacketHandler* out_;

  // Sources.
  std::vector<std::unique_ptr<BulkPacketSource>> sources_;

  // The queue that contains events.
  VectorPriorityQueue<Event, Comparator> queue_;

  // All packets will be tagged with this tag.
  PacketTag default_tag_;
};

class BulkPacketGenerator : public BulkPacketGeneratorBase,
                            public EventConsumer {
 public:
  // A number of packets are cached in a background thread, while the current
  // batch is being processed.
  static constexpr size_t kBatchSize = 10000;

  BulkPacketGenerator(const std::string& id,
                      std::vector<std::unique_ptr<BulkPacketSource>> sources,
                      htsim::PacketHandler* out, EventQueue* event_queue);

  ~BulkPacketGenerator();

  void HandleEvent() override;

  void StopQueueWhenDone() { stop_queue_when_done_ = true; }

 private:
  using Batch = std::vector<PacketPtr>;

  void GetNewBatchIfNeeded();

  // Adds initial events for each source.
  void Init();

  // Populates batches. Will block until no sources produce packets. Called by
  // batch_populator_.
  void PopulateBatches();

  // Adds the next packet to a batch.
  bool Next(Batch* out);

  // Passes batches between the thread that generates them and the one that
  // consumes them.
  PtrQueue<Batch, 1> ptr_queue_;

  // The current batch.
  Batch current_batch_;
  size_t current_batch_index_;

  // Populates batches.
  std::thread batch_populator_;

  bool stop_queue_when_done_;
};

class SingleThreadBulkPacketGenerator : public BulkPacketGeneratorBase,
                                        public EventConsumer {
 public:
  SingleThreadBulkPacketGenerator(
      const std::string& id,
      std::vector<std::unique_ptr<BulkPacketSource>> sources,
      htsim::PacketHandler* out, EventQueue* event_queue);

  void HandleEvent() override;

 private:
  void EnqueueNextPacket();

  PacketPtr next_pkt_;
};

// Generates a stream of UDP packets that have exponentially distributed gaps of
// time between them.
class ExpPacketSource : public htsim::BulkPacketSource {
 public:
  ExpPacketSource(const net::FiveTuple& five_tuple,
                  std::chrono::nanoseconds mean_time_between_packets,
                  size_t packet_size_bytes, size_t seed,
                  EventQueue* event_queue);

  htsim::PacketPtr NextPacket() override;

 private:
  net::FiveTuple five_tuple_;
  size_t pkt_size_;
  std::mt19937 generator_;
  std::exponential_distribution<double> distribution_;

  // Used to convert to/from time.
  EventQueue* event_queue_;
  EventQueueTime time_;
};

// Generates a constant stream of UDP packets.
class ConstantPacketSource : public htsim::BulkPacketSource {
 public:
  ConstantPacketSource(const net::FiveTuple& five_tuple,
                       std::chrono::nanoseconds mean_time_between_packets,
                       size_t packet_size_bytes, EventQueue* event_queue);

  PacketPtr NextPacket() override;

 private:
  net::FiveTuple five_tuple_;
  size_t pkt_size_;
  EventQueueTime gap_;
  EventQueueTime time_;
};

// A spike in load -- identified by its start time, duration and the rate it
// will achieve for the duration.
struct SpikeInTrafficLevel {
  std::chrono::milliseconds at;
  std::chrono::milliseconds duration;
  uint64_t rate_bps;
};

class SpikyPacketSource : public htsim::BulkPacketSource {
 public:
  SpikyPacketSource(const net::FiveTuple& five_tuple,
                    const std::vector<SpikeInTrafficLevel>& spikes,
                    size_t packet_size_bytes, EventQueue* event_queue);

  PacketPtr NextPacket() override;

 private:
  net::FiveTuple five_tuple_;
  size_t pkt_size_;

  // A list of spikes to be generated.
  std::vector<SpikeInTrafficLevel> spikes_;

  // Index into spikes_.
  size_t current_spike_;

  size_t packets_generated_from_current_spike_;

  EventQueue* event_queue_;
};

}  // namespace htsim
}  // namespace ncode
#endif
