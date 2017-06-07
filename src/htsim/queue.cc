#include "queue.h"

#include <algorithm>
#include <cmath>
#include <memory>

#include "../logging.h"
#include "../substitute.h"
#include "htsim.h"

namespace nc {
namespace htsim {

static std::string GetPipeId(const std::string& src, const std::string& dst) {
  return Substitute("pipe_$0_$1", src, dst);
}

static std::string GetQueueId(const std::string& src, const std::string& dst) {
  return Substitute("queue_$0_$1", src, dst);
}

Pipe::Pipe(const net::GraphLink& edge, EventQueue* event_queue)
    : Pipe(edge.src_node()->id(), edge.dst_node()->id(),
           event_queue->ToTime(edge.delay()), event_queue) {
  graph_link_ = &edge;
  other_end_ = nullptr;
}

Pipe::Pipe(const std::string& src, const std::string& dst, EventQueueTime delay,
           EventQueue* event_queue)
    : EventConsumer(GetPipeId(src, dst), event_queue),
      delay_(delay),
      other_end_(nullptr),
      graph_link_(nullptr) {}

void Pipe::HandleEvent() {
  PacketPtr pkt = std::move(queue_.front().second);
  queue_.pop_front();

  stats_.bytes_in_flight -= pkt->size_bytes();
  stats_.pkts_in_flight -= 1;
  stats_.bytes_tx += pkt->size_bytes();
  stats_.pkts_tx += 1;

  if (!queue_.empty()) {
    EventQueueTime next_event_time = queue_.front().first;
    EnqueueAt(next_event_time);
  }

  other_end_->HandlePacket(std::move(pkt));
}

void Pipe::HandlePacket(PacketPtr pkt) {
  if (queue_.empty()) {
    EnqueueIn(delay_);
  }

  uint32_t size_bytes = pkt->size_bytes();
  queue_.emplace_back(event_queue()->CurrentTime() + delay_, std::move(pkt));
  stats_.bytes_in_flight += size_bytes;
  stats_.pkts_in_flight += 1;
}

Queue::Queue(const std::string& src, const std::string& dst,
             EventQueue* event_queue)
    : EventConsumer(GetQueueId(src, dst), event_queue),
      other_end_(nullptr),
      bits_seen_in_last_period_(0) {}

Queue::Queue(const net::GraphLink& edge, EventQueue* event_queue)
    : Queue(edge.src_node()->id(), edge.dst_node()->id(), event_queue) {}

void Queue::ApplyValue(double value) {
  net::Bandwidth current_rate = GetRate();
  net::Bandwidth new_rate =
      net::Bandwidth::FromBitsPerSecond((static_cast<uint64_t>(value)));
  if (new_rate != current_rate) {
    SetRate(new_rate);
  }
}

void FIFOQueue::HandlePacket(PacketPtr pkt) {
  size_t size_bytes = pkt->size_bytes();
  if (ShouldDrop(size_bytes)) {
    stats_.bytes_dropped += size_bytes;
    stats_.pkts_dropped += 1;

    // Drop the packet (let the unique_ptr get out of scope).
    return;
  }

  // Rough estimate -- always includes all of the packets that are currently
  // being processed.
  EventQueueTime to_wait =
      EventQueueTime(time_per_bit_.Raw() * stats_.queue_size_bytes * 8);
  time_waiting_.Add(to_wait.Raw());

  bool queue_was_empty = queue_.empty();
  queue_.push_back(std::move(pkt));
  stats_.queue_size_bytes += size_bytes;
  stats_.queue_size_pkts += 1;
  stats_.bytes_seen += size_bytes;
  stats_.pkts_seen += 1;
  bits_seen_in_last_period_ += size_bytes * 8;

  if (queue_was_empty) {
    EnqueueIn(PacketDrainTime(*queue_.front()));
  }
}

void FIFOQueue::HandleEvent() {
  PacketPtr pkt = std::move(queue_.front());
  queue_.pop_front();

  stats_.queue_size_bytes -= pkt->size_bytes();
  stats_.queue_size_pkts -= 1;

  if (!queue_.empty()) {
    EnqueueIn(PacketDrainTime(*queue_.front()));
  }
  other_end_->HandlePacket(std::move(pkt));
}

void FIFOQueue::SetRate(net::Bandwidth new_rate) {
  rate_ = new_rate;
  CHECK(rate_ > net::Bandwidth::FromBitsPerSecond(0)) << "Zero rate at queue: "
                                                      << id();
  // TODO: make this independent of the event queue's clock resolution
  time_per_bit_ = EventQueueTime(pow(10, 12.0) / rate_.bps());
}

FIFOQueue::FIFOQueue(const std::string& src, const std::string& dst,
                     net::Bandwidth rate, uint64_t max_size_bytes,
                     EventQueue* event_queue)
    : Queue(src, dst, event_queue),
      max_size_bytes_(max_size_bytes),
      rate_(net::Bandwidth::FromBitsPerSecond(0)) {
  SetRate(rate);
}

FIFOQueue::FIFOQueue(const net::GraphLink& edge, uint64_t max_size_bytes,
                     EventQueue* event_queue)
    : FIFOQueue(edge.src_node()->id(), edge.dst_node()->id(), edge.bandwidth(),
                max_size_bytes, event_queue) {}

RandomQueue::RandomQueue(const std::string& src, const std::string& dst,
                         net::Bandwidth rate, uint64_t max_size_bytes,
                         uint64_t drop_threshold_bytes, double seed,
                         EventQueue* event_queue)
    : FIFOQueue(src, dst, rate, max_size_bytes, event_queue),
      drop_threshold_bytes_(drop_threshold_bytes),
      rnd_(seed) {
  CHECK(max_size_bytes > drop_threshold_bytes) << "Bad drop threshold: "
                                               << id();
}

RandomQueue::RandomQueue(const net::GraphLink& edge, uint64_t max_size_bytes,
                         uint64_t drop_threshold_bytes, double seed,
                         EventQueue* event_queue)
    : RandomQueue(edge.src_node()->id(), edge.dst_node()->id(),
                  edge.bandwidth(), max_size_bytes, drop_threshold_bytes, seed,
                  event_queue) {}

bool RandomQueue::ShouldDrop(uint64_t pkt_size_bytes) {
  double drop_prob = 0;
  uint64_t crt = stats_.queue_size_bytes + pkt_size_bytes;
  if (crt > max_size_bytes_) {
    return true;
  }

  if (crt > drop_threshold_bytes_) {
    drop_prob = static_cast<double>(crt - drop_threshold_bytes_) /
                (max_size_bytes_ - drop_threshold_bytes_);
    if (dis_(rnd_) < drop_prob) {
      return true;
    }
  }

  return false;
}

}  // namespace htsim
}  // namespace ncode
