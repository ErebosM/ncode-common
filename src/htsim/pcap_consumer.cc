#include "pcap_consumer.h"

#include <netinet/in.h>
#include <type_traits>

#include "../common.h"
#include "../event_queue.h"
#include "../free_list.h"
#include "../logging.h"

namespace nc {
namespace htsim {

PcapPacketGen::PcapPacketGen(
    std::unique_ptr<pcap::OfflineSourceProvider> source_provider,
    EventQueue* event_queue)
    : offline_pcap_(std::move(source_provider), this),
      max_interpacket_gap_(pcap::Timestamp::max()),
      time_shift_(pcap::Timestamp::zero()),
      prev_timestamp_(pcap::Timestamp::zero()),
      event_queue_(event_queue),
      break_(false),
      overwrite_ttl_(false) {}

void PcapPacketGen::HandleTCP(pcap::Timestamp timestamp,
                              const pcap::IPHeader& ip_header,
                              const pcap::TCPHeader& tcp_header,
                              uint16_t payload_len) {
  // All fields have to be converted to host-order.
  net::IPAddress src_address(ntohl(ip_header.ip_src.s_addr));
  net::IPAddress dst_address(ntohl(ip_header.ip_dst.s_addr));
  net::AccessLayerPort src_port(ntohs(tcp_header.th_sport));
  net::AccessLayerPort dst_port(ntohs(tcp_header.th_dport));
  size_t size = ntohs(ip_header.ip_len);
  net::FiveTuple five_tuple(src_address, dst_address, net::kProtoTCP, src_port,
                            dst_port);
  if (Ignore(five_tuple)) {
    return;
  }

  EventQueueTime time;
  if (!GetEventQueueTime(timestamp, &time)) {
    // If timestamps are not increasing will not only skip the packet, but break
    // out of the trace.
    break_ = true;
    return;
  }

  SeqNum seq_num(ntohl(tcp_header.th_seq));
  auto packet = GetFreeList<TCPPacket>().New(five_tuple, size, time, seq_num);
  packet->set_id(ntohs(ip_header.ip_id));
  packet->set_flags(tcp_header.th_flags);
  packet->set_payload(payload_len);
  packet->set_ttl(overwrite_ttl_ ? kDefaultTTL : ip_header.ip_ttl);
  pending_packet_ = std::move(packet);
}

void PcapPacketGen::HandleUDP(pcap::Timestamp timestamp,
                              const pcap::IPHeader& ip_header,
                              const pcap::UDPHeader& udp_header,
                              uint16_t payload_len) {
  net::IPAddress src_address(ntohl(ip_header.ip_src.s_addr));
  net::IPAddress dst_address(ntohl(ip_header.ip_dst.s_addr));
  net::AccessLayerPort src_port(ntohs(udp_header.uh_sport));
  net::AccessLayerPort dst_port(ntohs(udp_header.uh_dport));
  size_t size = ntohs(ip_header.ip_len);
  net::FiveTuple five_tuple(src_address, dst_address, net::kProtoUDP, src_port,
                            dst_port);
  if (Ignore(five_tuple)) {
    return;
  }

  EventQueueTime time;
  if (!GetEventQueueTime(timestamp, &time)) {
    break_ = true;
    return;
  }

  auto packet = GetFreeList<UDPPacket>().New(five_tuple, size, time);
  packet->set_id(ntohs(ip_header.ip_id));
  packet->set_payload(payload_len);
  packet->set_ttl(overwrite_ttl_ ? kDefaultTTL : ip_header.ip_ttl);
  pending_packet_ = std::move(packet);
}

void PcapPacketGen::set_max_inter_packet_gap(pcap::Timestamp gap) {
  max_interpacket_gap_ = gap;
}

PacketPtr PcapPacketGen::NextPacket() {
  while (!pending_packet_) {
    if (!offline_pcap_.NextPacket() || break_) {
      return PacketPtr();
    }
  }

  return std::move(pending_packet_);
}

bool PcapPacketGen::GetEventQueueTime(pcap::Timestamp timestamp,
                                      EventQueueTime* time) {
  // The first packet from the trace will arrive at 0, and all other packets
  // will be offset accordingly.
  EventQueueTime now = EventQueueTime::ZeroTime();
  if (init_timestamp_ == pcap::Timestamp::max()) {
    init_timestamp_ = timestamp;
    prev_timestamp_ = timestamp;
  } else {
    if (prev_timestamp_ > timestamp) {
      LOG(ERROR) << "Timestamps not increasing: negtaive delta "
                 << (prev_timestamp_ - timestamp).count() << "ns";
      return false;
    }

    pcap::Timestamp delta_from_last = timestamp - prev_timestamp_;
    if (delta_from_last >= max_interpacket_gap_) {
      time_shift_ += delta_from_last;
      LOG(INFO) << "Added time shift of " << delta_from_last.count()
                << " nanos";
    }
    prev_timestamp_ = timestamp;

    CHECK(timestamp >= init_timestamp_);
    pcap::Timestamp delta_from_start = timestamp - init_timestamp_;
    CHECK(delta_from_start >= time_shift_) << "ds " << delta_from_start.count()
                                           << " vs ts " << time_shift_.count();
    delta_from_start -= time_shift_;

    now = event_queue_->ToTime(delta_from_start);
  }

  *time = now;
  return true;
}

}  // namespace htsim
}  // namespace nc
