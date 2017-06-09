#ifndef HTSIM_PACKET_H
#define HTSIM_PACKET_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "../common.h"
#include "../event_queue.h"
#include "../net/net_common.h"
#include "../net/pcap.h"
#include "htsim.h"

namespace nc {
namespace htsim {
class MatchRule;
} /* namespace htsim */
} /* namespace ncode */

namespace nc {
namespace htsim {

// A 32 bit integer tag that can go on a packet.
struct PacketTagTag {};
using PacketTag = TypesafeUintWrapper<PacketTagTag, uint32_t, '*'>;

// A sequence number.
struct SeqNumTag {};
using SeqNum = TypesafeUintWrapper<SeqNumTag, uint64_t>;

static constexpr PacketTag kDefaultTag = PacketTag(0);
static constexpr uint8_t kDefaultTTL = 100;

class Packet;

// All packets have this pointer type.
using PacketPtr = std::unique_ptr<Packet>;

// A generic packet in the simulation.
class Packet {
 public:
  virtual ~Packet() {}

  // Each packet carries a FiveTuple.
  const net::FiveTuple& five_tuple() const { return five_tuple_; }

  // The size of the packet in bytes.
  uint16_t size_bytes() const { return size_bytes_; }

  // The IP id. Not guaranteed to be set.
  uint16_t ip_id() const { return ip_id_; }

  // The tag of the packet. Each packet can be tagged with at most one tag. If
  // the value of this is kDefaultTag the packet is untagged.
  PacketTag tag() const { return tag_; }

  // The time the packet was sent is recorded.
  EventQueueTime time_sent() const { return time_sent_; }

  // TTL value for the packet. This starts at kDefaultTTL and is decremented
  // each hop until it gets to 0, at which point the packet is dropped.
  uint8_t ttl() const { return ttl_; }

  // Returns the payload (if set) carried by this packet. By default the
  // packet's payload is the same as its size.
  uint16_t payload_bytes() const { return payload_bytes_; }

  // Whether or not this packet should be dropped before any non-preferential
  // drop packets are dropped at queues.
  bool preferential_drop() const { return preferential_drop_; }

  // Tags this packet.
  void set_tag(PacketTag tag) { tag_ = tag; }

  // Sets the IP id.
  void set_id(uint16_t id) { ip_id_ = id; }

  // Sets the TTL value.
  void set_ttl(uint8_t ttl) { ttl_ = ttl; }

  // Sets the payload.
  void set_payload(uint16_t payload_bytes) { payload_bytes_ = payload_bytes; }

  // Sets preferential dropping for the packet.
  void set_preferential_drop(bool preferential_drop) {
    preferential_drop_ = preferential_drop;
  }

  // Decrements TTL and returns true if it is still greater than 0.
  bool DecrementTTL();

  bool is_tcp() const { return five_tuple_.ip_proto() == nc::net::kProtoTCP; }
  bool is_udp() const { return five_tuple_.ip_proto() == nc::net::kProtoUDP; }
  bool is_icmp() const { return five_tuple_.ip_proto() == nc::net::kProtoICMP; }

  // Creates a copy of the packet.
  virtual PacketPtr Duplicate() const = 0;

  // A human-readable description of the contents of the packet.
  virtual std::string ToString() const = 0;

 protected:
  Packet(const net::FiveTuple& five_tuple, uint16_t size_bytes,
         EventQueueTime time_sent);

  net::FiveTuple five_tuple_;
  uint16_t size_bytes_;
  uint16_t ip_id_;
  PacketTag tag_;
  uint8_t ttl_;
  EventQueueTime time_sent_;
  bool preferential_drop_;
  uint16_t payload_bytes_;
};

// A TCP packet. The same packet object is used for regular TCP packets as for
// ACKs.
class TCPPacket : public Packet {
 public:
  TCPPacket(net::FiveTuple five_tuple, uint16_t size_bytes,
            EventQueueTime time_sent, SeqNum sequence);

  // The sequence number.
  SeqNum sequence() const { return sequence_; }

  PacketPtr Duplicate() const override;

  std::string ToString() const override;

  void set_flags(uint8_t flags) { flags_ = flags; }
  uint8_t flags() const { return flags_; }

  bool syn() const { return flags_ & nc::pcap::TCPHeader::kSynFlag; }

  bool ack() const { return flags_ & nc::pcap::TCPHeader::kAckFlag; }

  bool fin() const { return flags_ & nc::pcap::TCPHeader::kFinFlag; }

 private:
  SeqNum sequence_;

  // If this packet comes from a real-world trace this field will be set to the
  // flags of the TCP packet. If not it will be 0.
  uint8_t flags_;
};

// A UDP packet.
class UDPPacket : public Packet {
 public:
  UDPPacket(net::FiveTuple five_tuple, uint16_t size_bytes,
            EventQueueTime time_sent);

  PacketPtr Duplicate() const override;

  std::string ToString() const override;
};

// An interface for any class that can handle packets.
class PacketHandler {
 public:
  virtual ~PacketHandler() {}
  virtual void HandlePacket(PacketPtr pkt) = 0;

 protected:
  PacketHandler() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(PacketHandler);
};

// A handler that does nothing.
class DummyPacketHandler : public PacketHandler {
 public:
  void HandlePacket(PacketPtr pkt) override { nc::Unused(pkt); }
};

// Like PacketHandler, but does not take ownership of the packet.
class PacketObserver {
 public:
  virtual ~PacketObserver() {}
  virtual void ObservePacket(const Packet& pkt) = 0;

 protected:
  PacketObserver() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(PacketObserver);
};

// Statistics about a Connection (below).
struct ConnectionStats {
  uint64_t pkts_tx = 0;
  uint64_t pkts_rx = 0;
  uint64_t bytes_tx = 0;
  uint64_t bytes_rx = 0;
};

// A connection is an object that can receive / transmit packets. Data can be
// added to it and it can be closed. The connection is usually attached to the
// loopback port on a device. All connections are controlled by an orchestrator
// that adds data to them.
class Connection : public PacketHandler, public SimComponent {
 public:
  const ConnectionStats& GetStats() const { return stats_; }

  void HandlePacket(PacketPtr pkt) final;

  // Inserts data into this connection. In the case of UDP this will cause a
  // packet to be generated immediately. In the case of TCP the data will go
  // into the transmit buffer. If the connection is inactive (has been closed or
  // has not transmitted any data yet) it will be started.
  virtual void AddData(uint64_t data_bytes) = 0;

  // Closes the connection. Can be re-opened by subsequent calls to AddData.
  virtual void Close() = 0;

  // Provides a callback to be called when the buffer is drained.
  void OnSendBufferDrained(std::function<void()> on_send_buffer_drained) {
    on_send_buffer_drained_ = on_send_buffer_drained;
  }

 protected:
  virtual void ReceivePacket(PacketPtr pkt) = 0;

  Connection(const std::string& id, const net::FiveTuple& five_tuple,
             PacketHandler* out, EventQueue* event_queue);

  // Sends a packet out.
  void SendPacket(PacketPtr pkt);

  // All packets generated by this generator will have this tuple.
  const net::FiveTuple five_tuple_;

  // A callback to be called when the TX buffer has been drained.
  std::function<void()> on_send_buffer_drained_;

 private:
  // Packets generated by this connection are sent here, non-owning pointer
  PacketHandler* out_;

  // Stats about the connection.
  ConnectionStats stats_;

  DISALLOW_COPY_AND_ASSIGN(Connection);
};

// The message is a special packet that has always size 0 (which means that
// queues are a no-op for them). Messages can still be used with pipes to
// simulate delay. This is useful if you want to mix packet-based simulation
// with a message-based communication channel. One example are the devices in
// network.h which are configured using messages, but pass packets. No regular
// packets should have size 0 and when a PacketHandler receives a packet it can
// check the size to figure out if it is a message. The message also has no
// src/dst port and ip protocol field is used as a generic type field.
class Message : public Packet {
 protected:
  Message(net::IPAddress ip_src, net::IPAddress ip_dst, uint8_t message_type,
          EventQueueTime time_sent);

  virtual PacketPtr Duplicate() const override = 0;
};

// A configuration message for a simulated switch.
class SSCPMessage : public Message {
 public:
  static constexpr uint64_t kNoTxId = 0;

  // Gets/sets the TX id of the message. If the TX id is set to something
  // different from the default kNoTxId the switch will reply to it with an ack
  // that will carry the same tx id.
  uint64_t tx_id() const { return tx_id_; }
  void set_tx_id(uint64_t tx_id) { tx_id_ = tx_id; }

 protected:
  SSCPMessage(net::IPAddress ip_src, net::IPAddress ip_dst,
              uint8_t message_type, EventQueueTime time_sent);

 private:
  // Pairs SSCPMessage and SSCPReply.
  uint64_t tx_id_;
};

// Adds or updates a single rule. Check the comments in matcher.h to see how
// the update works.
class SSCPAddOrUpdate : public SSCPMessage {
 public:
  static constexpr uint8_t kSSCPAddOrUpdateType = 180;

  SSCPAddOrUpdate(net::IPAddress ip_src, net::IPAddress ip_dst,
                  EventQueueTime time_sent, std::unique_ptr<MatchRule> rule);

  PacketPtr Duplicate() const override;

  const MatchRule& rule() const { return *(rule_.get()); }

  // Returns ownership of the rule.
  std::unique_ptr<MatchRule> TakeRule();

  std::string ToString() const override;

 private:
  std::unique_ptr<MatchRule> rule_;
};

class SSCPAck : public SSCPMessage {
 public:
  static constexpr uint8_t kSSCPAckType = 254;

  SSCPAck(net::IPAddress ip_src, net::IPAddress ip_dst,
          EventQueueTime time_sent, uint64_t tx_id);

  PacketPtr Duplicate() const override;

  std::string ToString() const override;
};

}  // namespace htsim
}  // namespace nc

#endif
