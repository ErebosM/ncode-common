#ifndef HTSIM_PHYSICAL_H
#define HTSIM_PHYSICAL_H

#include <pcap/pcap.h>
#include <stddef.h>
#include <iostream>

#include "../net/pcap.h"
#include "packet.h"

namespace nc {
namespace htsim {

// Creates a real-life physical packet for a simulated one and sends it out a
// given interface. Packets will not be sent immediately, but on batches because
// of performance.
class PhysicalInterfacePacketHandler : public PacketHandler,
                                       public EventConsumer {
 public:
  static constexpr std::chrono::microseconds kTickDuration =
      std::chrono::microseconds(100);

  PhysicalInterfacePacketHandler(const std::string& iface_name,
                                 EventQueue* event_queue);

  void HandlePacket(PacketPtr pkt) override;

  void HandleEvent() override;

 private:
  static constexpr size_t kSendBufferSize = 2048;
  static constexpr size_t kMTUSizeBytes = 1000;

  void SendPacket(PacketPtr pkt);

  void SendUDP(const UDPPacket& udp_packet);

  // The pcap handle.
  pcap_t* pcap_;

  // The sending buffer. Scratch space.
  char sendbuf_[kSendBufferSize];

  // Pointer to the ip header in sendbuf_.
  pcap::IPHeader* iph_;

  // If the interface is a loopback we have to use a null link-layer header
  // instead of the ethernet one. This field gives the size of the link-layer
  // header.
  uint8_t ll_header_size_;

  // Packets in the current batch.
  std::vector<PacketPtr> packets_in_batch_;
};

}  // namespace htsim
}  // namespace nc

#endif
