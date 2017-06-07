#include "bulk_gen.h"

#include <type_traits>

#include "../logging.h"

namespace nc {
namespace htsim {

BulkPacketGenerator::BulkPacketGenerator(
    const std::string& id,
    std::vector<std::unique_ptr<BulkPacketSource>> sources,
    htsim::PacketHandler* out, EventQueue* event_queue)
    : BulkPacketGeneratorBase(std::move(sources), out),
      EventConsumer(id, event_queue),
      current_batch_index_(0),
      stop_queue_when_done_(false) {
  Init();
}

BulkPacketGenerator::~BulkPacketGenerator() {
  ptr_queue_.Close();
  batch_populator_.join();
}

void BulkPacketGenerator::GetNewBatchIfNeeded() {
  if (current_batch_index_ == current_batch_.size()) {
    auto incoming_batch = ptr_queue_.ConsumeOrBlock();
    if (!incoming_batch) {
      return;
    }

    current_batch_.swap(*incoming_batch);
    current_batch_index_ = 0;
  }
}

void BulkPacketGenerator::HandleEvent() {
  PacketPtr packet = std::move(current_batch_[current_batch_index_]);
  CHECK(packet) << "Bad packet";
  packet->set_tag(default_tag_);
  out_->HandlePacket(std::move(packet));

  ++current_batch_index_;
  GetNewBatchIfNeeded();
  if (current_batch_.empty()) {
    if (stop_queue_when_done_) {
      event_queue()->Stop();
    }
    return;
  }

  PacketPtr& next_packet = current_batch_[current_batch_index_];
  EventQueueTime at = next_packet->time_sent();
  EnqueueAt(at);
}

void BulkPacketGenerator::PopulateBatches() {
  while (true) {
    auto batch = make_unique<Batch>();
    for (size_t i = 0; i < kBatchSize; ++i) {
      if (!Next(batch.get())) {
        break;
      }
    }

    if (!ptr_queue_.ProduceOrBlock(std::move(batch))) {
      return;
    }
  }
}

bool BulkPacketGenerator::Next(Batch* out) {
  PacketPtr next_pkt = NextPacket();
  if (!next_pkt) {
    return false;
  }

  out->emplace_back(std::move(next_pkt));
  return true;
}

SingleThreadBulkPacketGenerator::SingleThreadBulkPacketGenerator(
    const std::string& id,
    std::vector<std::unique_ptr<BulkPacketSource>> sources,
    htsim::PacketHandler* out, EventQueue* event_queue)
    : BulkPacketGeneratorBase(std::move(sources), out),
      EventConsumer(id, event_queue) {
  AddInitialEvents();
  EnqueueNextPacket();
}

void SingleThreadBulkPacketGenerator::EnqueueNextPacket() {
  next_pkt_ = NextPacket();
  if (next_pkt_) {
    EventQueueTime at = next_pkt_->time_sent();
    EnqueueAt(at);
  }
}

void SingleThreadBulkPacketGenerator::HandleEvent() {
  if (!next_pkt_) {
    return;
  }

  out_->HandlePacket(std::move(next_pkt_));
  EnqueueNextPacket();
}

BulkPacketGeneratorBase::BulkPacketGeneratorBase(
    std::vector<std::unique_ptr<BulkPacketSource>> sources,
    htsim::PacketHandler* out)
    : out_(out), sources_(std::move(sources)), default_tag_(kDefaultTag) {
  for (const auto& src : sources_) {
    CHECK(src) << "Empty source";
  }
}

PacketPtr BulkPacketGeneratorBase::NextPacket() {
  if (queue_.empty()) {
    return PacketPtr();
  }

  Event ev = queue_.PopTop();
  PacketPtr to_return = std::move(ev.pkt);

  BulkPacketSource* source = ev.source;
  AddEventFromSource(source);
  return to_return;
}

void BulkPacketGeneratorBase::AddEventFromSource(BulkPacketSource* source) {
  PacketPtr next_pkt = source->NextPacket();
  if (next_pkt) {
    queue_.emplace(std::move(next_pkt), source);
  }
}

void BulkPacketGenerator::Init() {
  AddInitialEvents();

  batch_populator_ = std::thread([this] { PopulateBatches(); });
  GetNewBatchIfNeeded();
  if (current_batch_.empty()) {
    return;
  }

  PacketPtr& next_packet = current_batch_[current_batch_index_];
  EventQueueTime at = next_packet->time_sent();
  EnqueueAt(at);
}

void BulkPacketGeneratorBase::AddInitialEvents() {
  for (const auto& source : sources_) {
    AddEventFromSource(source.get());
  }
}

ExpPacketSource::ExpPacketSource(
    const net::FiveTuple& five_tuple,
    std::chrono::nanoseconds mean_time_between_packets,
    size_t packet_size_bytes, size_t seed, EventQueue* event_queue)
    : five_tuple_(five_tuple),
      pkt_size_(packet_size_bytes),
      generator_(seed),
      distribution_(1.0 / mean_time_between_packets.count()),
      event_queue_(event_queue) {
  CHECK(five_tuple.ip_proto() == net::kProtoUDP);
}

htsim::PacketPtr ExpPacketSource::NextPacket() {
  size_t delta_nanos = distribution_(generator_);
  EventQueueTime delta =
      event_queue_->ToTime(std::chrono::nanoseconds(delta_nanos));

  time_ += delta;
  return make_unique<htsim::UDPPacket>(five_tuple_, pkt_size_, time_);
}

ConstantPacketSource::ConstantPacketSource(
    const net::FiveTuple& five_tuple,
    std::chrono::nanoseconds mean_time_between_packets,
    size_t packet_size_bytes, EventQueue* event_queue)
    : five_tuple_(five_tuple),
      pkt_size_(packet_size_bytes),
      gap_(event_queue->ToTime(mean_time_between_packets)) {
  CHECK(five_tuple.ip_proto() == net::kProtoUDP);
}

PacketPtr ConstantPacketSource::NextPacket() {
  time_ += gap_;
  return make_unique<htsim::UDPPacket>(five_tuple_, pkt_size_, time_);
}

SpikyPacketSource::SpikyPacketSource(
    const net::FiveTuple& five_tuple,
    const std::vector<SpikeInTrafficLevel>& spikes, size_t packet_size_bytes,
    EventQueue* event_queue)
    : five_tuple_(five_tuple),
      pkt_size_(packet_size_bytes),
      spikes_(spikes),
      current_spike_(0),
      packets_generated_from_current_spike_(0),
      event_queue_(event_queue) {}

PacketPtr SpikyPacketSource::NextPacket() {
  if (current_spike_ == spikes_.size()) {
    return PacketPtr();
  }
  const SpikeInTrafficLevel& spike = spikes_[current_spike_];

  double rate_Bps = spike.rate_bps / 8.0;
  double pps = rate_Bps / pkt_size_;
  EventQueueTime gap = event_queue_->ToTime(std::chrono::seconds(1)) / pps;
  EventQueueTime time_into_spike = gap * packets_generated_from_current_spike_;

  // Need to figure out if we should switch to the next spike.
  ++packets_generated_from_current_spike_;
  if (time_into_spike > event_queue_->ToTime(spike.duration)) {
    packets_generated_from_current_spike_ = 0;
    ++current_spike_;
    return NextPacket();
  }

  return make_unique<htsim::UDPPacket>(
      five_tuple_, pkt_size_, event_queue_->ToTime(spike.at) + time_into_spike);
}

}  // namespace htsim
}  // namesapce ncode
