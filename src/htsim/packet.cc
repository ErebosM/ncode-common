#include "packet.h"

#include "../logging.h"
#include "../substitute.h"
#include "match.h"

namespace nc {
namespace htsim {

Packet::Packet(const net::FiveTuple& five_tuple, uint32_t size_bytes,
               EventQueueTime time_sent)
    : five_tuple_(five_tuple),
      size_bytes_(size_bytes),
      ip_id_(0),
      tag_(kDefaultTag),
      ttl_(kDefaultTTL),
      time_sent_(time_sent),
      preferential_drop_(false),
      payload_bytes_(size_bytes) {}

bool Packet::DecrementTTL() {
  if (ttl_ == 0) {
    return false;
  }
  --ttl_;
  return true;
}

TCPPacket::TCPPacket(net::FiveTuple five_tuple, uint32_t size_bytes,
                     EventQueueTime time_sent, SeqNum sequence)
    : Packet(five_tuple, size_bytes, time_sent),
      sequence_(sequence),
      flags_(0) {
  CHECK(size_bytes > 0) << "0-size TCP packet";
}

PacketPtr TCPPacket::Duplicate() const {
  auto new_pkt = GetFreeList<TCPPacket>().New(five_tuple_, size_bytes_,
                                              time_sent_, sequence_);
  new_pkt->ip_id_ = ip_id_;
  new_pkt->tag_ = tag_;
  new_pkt->ttl_ = ttl_;
  new_pkt->preferential_drop_ = preferential_drop_;
  new_pkt->payload_bytes_ = payload_bytes_;
  new_pkt->queueing_time_ = queueing_time_;
  new_pkt->flags_ = flags_;
  return std::move(new_pkt);
}

std::string TCPPacket::ToString() const {
  return Substitute("$0 tag $1: TCP $2", five_tuple_.ToString(), tag().Raw(),
                    sequence_.Raw());
}

UDPPacket::UDPPacket(net::FiveTuple five_tuple, uint32_t size_bytes,
                     EventQueueTime time_sent)
    : Packet(five_tuple, size_bytes, time_sent) {
  CHECK(size_bytes > 0) << "0-size UDP packet";
}

PacketPtr UDPPacket::Duplicate() const {
  auto new_pkt =
      GetFreeList<UDPPacket>().New(five_tuple_, size_bytes_, time_sent_);
  new_pkt->ip_id_ = ip_id_;
  new_pkt->tag_ = tag_;
  new_pkt->ttl_ = ttl_;
  new_pkt->preferential_drop_ = preferential_drop_;
  new_pkt->payload_bytes_ = payload_bytes_;
  new_pkt->queueing_time_ = queueing_time_;
  return std::move(new_pkt);
}

std::string UDPPacket::ToString() const {
  return Substitute("$0 tag $1: UDP", five_tuple_.ToString(), tag().Raw());
}

void Connection::HandlePacket(PacketPtr pkt) {
  stats_.bytes_rx += pkt->size_bytes();
  stats_.pkts_rx += 1;
  ReceivePacket(std::move(pkt));
}

void Connection::SendPacket(PacketPtr pkt) {
  stats_.bytes_tx += pkt->size_bytes();
  stats_.pkts_tx += 1;
  out_->HandlePacket(std::move(pkt));
}

Connection::Connection(const std::string& id, const net::FiveTuple& five_tuple,
                       PacketHandler* out, EventQueue* event_queue)
    : SimComponent(id, event_queue), five_tuple_(five_tuple), out_(out) {}

Message::Message(net::IPAddress ip_src, net::IPAddress ip_dst,
                 uint8_t message_type, EventQueueTime time_sent)
    : Packet({ip_src, ip_dst, net::IPProto(message_type), kWildAccessLayerPort,
              kWildAccessLayerPort},
             0, time_sent) {}

SSCPMessage::SSCPMessage(net::IPAddress ip_src, net::IPAddress ip_dst,
                         uint8_t message_type, EventQueueTime time_sent)
    : Message(ip_src, ip_dst, message_type, time_sent), tx_id_(kNoTxId) {}

SSCPAddOrUpdate::SSCPAddOrUpdate(net::IPAddress ip_src, net::IPAddress ip_dst,
                                 EventQueueTime time_sent,
                                 std::unique_ptr<MatchRule> rule)
    : SSCPMessage(ip_src, ip_dst, kSSCPAddOrUpdateType, time_sent),
      rule_(std::move(rule)) {
  CHECK(rule_) << "Empty rule";
}

PacketPtr SSCPAddOrUpdate::Duplicate() const {
  auto rule = rule_->Clone();
  auto new_msg = GetFreeList<SSCPAddOrUpdate>().New(
      five_tuple_.ip_src(), five_tuple_.ip_dst(), time_sent_, std::move(rule));
  new_msg->set_tx_id(tx_id());
  return std::move(new_msg);
}

std::string SSCPAddOrUpdate::ToString() const {
  std::stringstream ss;
  ss << *rule_;

  return Substitute("MSG $0 -> $1 : SSCP $2 tx id $3",
                    net::IPToStringOrDie(five_tuple_.ip_src()),
                    net::IPToStringOrDie(five_tuple_.ip_dst()), ss.str(),
                    tx_id());
}

std::unique_ptr<MatchRule> SSCPAddOrUpdate::TakeRule() {
  return std::move(rule_);
}

SSCPAck::SSCPAck(net::IPAddress ip_src, net::IPAddress ip_dst,
                 EventQueueTime time_sent, uint64_t tx_id)
    : SSCPMessage(ip_src, ip_dst, kSSCPAckType, time_sent) {
  set_tx_id(tx_id);
}

PacketPtr SSCPAck::Duplicate() const {
  auto new_msg = GetFreeList<SSCPAck>().New(
      five_tuple_.ip_src(), five_tuple_.ip_dst(), time_sent_, tx_id());
  return std::move(new_msg);
}

std::string SSCPAck::ToString() const {
  return Substitute("MSG $0 -> $1 : SSCPAck $2",
                    net::IPToStringOrDie(five_tuple_.ip_src()),
                    net::IPToStringOrDie(five_tuple_.ip_dst()), tx_id());
}

}  // namespace htsim
}  // namespace ncode
